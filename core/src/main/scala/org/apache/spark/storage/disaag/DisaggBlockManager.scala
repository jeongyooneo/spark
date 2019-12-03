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
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.disaag.DisaggBlockManagerMessages._
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.util.io.ChunkedByteBuffer

private[spark] class DisaggBlockManager(
      var driverEndpoint: RpcEndpointRef,
      conf: SparkConf) extends Logging with CrailManager {

  def createFile(blockId: BlockId) : CrailFile = {
    logInfo("jy: disagg: fresh file, writing " + blockId.name)
    val path = getPath(blockId)
    val fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().asFile()

    driverEndpoint.ask[Boolean](FileCreated(blockId))
    fileInfo
  }

  def getFile(blockId: BlockId): CrailFile = {
    val path = getPath(blockId)
    fs.lookup(path).get().asFile()
  }

  def remove(blockId: BlockId): Boolean = {
    if (blockExists(blockId)) {
      val path = getPath(blockId)
      fs.delete(path, false).get().syncDir()
      logInfo(s"jy: Removed block $blockId from disagg")

      driverEndpoint.askSync[Boolean](FileRemoved(blockId))
    } else {
      false
    }
  }

  def blockExists(blockId: BlockId): Boolean = {
    driverEndpoint.askSync[Boolean](Contains(blockId))
  }

  def getSize(blockId: BlockId): Long = {
    val size = driverEndpoint.askSync[Long](GetSize(blockId))
    logInfo(s"Disagg get block size $blockId, size $size")
    size
  }

  def writeEnd(blockId: BlockId, size: Long): Boolean = {
    driverEndpoint.askSync[Boolean](FileWriteEnd(blockId, size))
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
            blockInfoManager: BlockInfoManager): DisaggCachingPolicy = {
     val cachingPolicyType = conf.get("spark.disagg.cachingpolicy", "None")
     cachingPolicyType match {
      case "None" => new NoCachingPolicy(memoryStore)
      case "Local" => new LocalCachingPolicy(memoryStore, blockInfoManager)
      case _ => throw new RuntimeException("Invalid caching policy " + cachingPolicyType)
    }
  }
}
