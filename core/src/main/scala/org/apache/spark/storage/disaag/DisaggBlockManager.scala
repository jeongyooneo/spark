/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage.disaag

import org.apache.crail._
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.disaag.DisaggBlockManagerMessages._
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.concurrent.duration.Duration

class DisaggBlockManager(
      var driverEndpoint: RpcEndpointRef,
      conf: SparkConf) extends Logging with CrailManager {

  def discardBlocksIfNecessary(estimateSize: Long) : Unit = {
    driverEndpoint.askSync[Boolean](DiscardBlocksIfNecessary(estimateSize))
  }

  def storeBlockOrNot(blockId: BlockId, estimateSize: Long): Boolean = {

    val taskContext = TaskContext.get()
    val taskId = s"${taskContext.stageId()}-" +
      s"${taskContext.partitionId()}-${taskContext.attemptNumber()}"
    driverEndpoint.askSync[Boolean](StoreBlockOrNot(blockId, estimateSize, taskId))
  }

  def read(blockId: BlockId) : Boolean = {
    val result = driverEndpoint.askSync[Int](FileRead(blockId))

    logInfo(s"Read logging ... $blockId, result: $result")

    if (result == 1) {
      true
    } else if (result == 2) {
      // retry... the block is being written
      Thread.sleep(500)
      read(blockId)
    } else if (result == 0) {
      false
    } else {
      throw new RuntimeException(s"Invalid read value $result for reading $blockId")
    }
  }

  def readUnlock(blockId: BlockId) : Unit = {
    driverEndpoint.ask[Unit](FileReadUnlock(blockId))
  }

  def createFile(blockId: BlockId) : CrailFile = {
    val path = getPath(blockId)

    logInfo("jy: disagg: getting result for create file " + blockId.name)

    val result = ThreadUtils.awaitResult(
      driverEndpoint.ask[Boolean](FileCreated(blockId)), Duration(10000, "millis"))

    if (result) {
      val fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, true).get().asFile()
      logInfo("jy: disagg: fresh file, writing " + blockId.name)
      fileInfo
    } else {
      logInfo("jy: disagg: file exists, return null " + blockId.name)
      null
    }
  }

  def getFile(blockId: BlockId): CrailFile = {
    val path = getPath(blockId)
    fs.lookup(path).get().asFile()
  }

  def remove(blockId: BlockId): Boolean = {
    if (blockExists(blockId)) {
      val path = getPath(blockId)
      fs.delete(path, false)
      logInfo(s"jy: Removed block $blockId from disagg")

      logInfo(s"Removed block $blockId lookup ${fs.lookup(path).get()}")

      driverEndpoint.ask(FileRemoved(blockId))
      true
    } else {
      logInfo(s"Block $blockId is already removed from disagg")
      false
    }
  }

  def blockExists(blockId: BlockId): Boolean = {
    try {
      var result = driverEndpoint.askSync[Int](Contains(blockId))
      while (result == 2) {
        logInfo(s"blockExist check again $blockId")
        Thread.sleep(1000)
        result = driverEndpoint.askSync[Int](Contains(blockId))
      }
      result == 1
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException(e)

    }
  }

  def getSize(blockId: BlockId): Long = {
    val size = driverEndpoint.askSync[Long](GetSize(blockId))
    logInfo(s"Disagg get block size $blockId, size $size")
    size
  }

  def writeEnd(blockId: BlockId, size: Long): Unit = {
    logInfo(s"jy: Writing end $blockId from disagg")
    driverEndpoint.ask(FileWriteEnd(blockId, size))
  }
}

abstract class DisaggStoringPolicy() extends Logging {

  def isStoringEvictedBlockToDisagg(blockId: BlockId): Boolean

}

object DisaggStoringPolicy {
  def apply(conf: SparkConf): DisaggStoringPolicy = {
    val storingPolicy = conf.get("spark.disagg.storingpolicy", "Default")
    storingPolicy match {
      case "Default" => new DefaultDisaggStoringPolicy()
      case "No" => new NoStoringEvictBlockDPolicy()
      case "RDD2" => new Rdd2DisaggStoringPolicy()
      case _ => throw new RuntimeException("Invalid storing policy " + storingPolicy)
    }
  }
}

abstract class DisaggCachingPolicy(
           memoryStore: MemoryStore) extends Logging{


  /**
   * Attempts to cache spilled values read from disagg into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  def maybeCacheDisaggValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      disaggIterator: Iterator[T]): Iterator[T]

  /**
   * Attempts to cache spilled bytes read from disagg into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  def maybeCacheDisaggBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      disaggData: BlockData): Option[ChunkedByteBuffer]


}

object DisaggCachingPolicy {

  def apply(conf: SparkConf,
            memoryStore: MemoryStore,
            blockInfoManager: BlockInfoManager,
            disaggStore: DisaggStore): DisaggCachingPolicy = {
     val cachingPolicyType = conf.get("spark.disagg.cachingpolicy", "None")
     cachingPolicyType match {
      case "None" => new NoCachingPolicy(memoryStore)
      case "Local" => new LocalCachingPolicy(memoryStore, blockInfoManager, disaggStore)
      case _ => throw new RuntimeException("Invalid caching policy " + cachingPolicyType)
    }
  }
}
