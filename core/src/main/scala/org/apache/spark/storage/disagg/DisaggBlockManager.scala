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

package org.apache.spark.storage.disagg

import java.util.concurrent.ConcurrentHashMap

import org.apache.crail._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.convert.decorateAsScala._

class DisaggBlockManager(
      var driverEndpoint: RpcEndpointRef,
      conf: SparkConf) extends CrailManager(false,
  false) with Logging {

  def discardBlocksIfNecessary(estimateSize: Long) : Unit = {
    driverEndpoint.askSync[Boolean](DiscardBlocksIfNecessary(estimateSize))
  }

  private val isRddCacheMap = new ConcurrentHashMap[Int, Boolean]().asScala
  private var prevUpdate = System.currentTimeMillis()
  private val expiredPeriod = 2000

  def isRDDCache(rddId: Int): Boolean = {

    val curr = System.currentTimeMillis()
    if (curr - prevUpdate >= expiredPeriod) {
      isRddCacheMap.clear()
      prevUpdate = curr
    }

    if (isRddCacheMap.contains(rddId)) {
      isRddCacheMap(rddId)
    } else {
      val result = driverEndpoint.askSync[Boolean](IsRddCache(rddId))
      isRddCacheMap(rddId) = result
      result
    }
  }

  private var localEvictionTime = System.currentTimeMillis()
  private var localEvictionCache: List[BlockId] = List.empty[BlockId]

  def localEviction(blockId: Option[BlockId], executorId: String, size: Long,
                    prevEvicted: Set[BlockId],
                    onDisk: Boolean): List[BlockId] = {
      driverEndpoint.askSync[List[BlockId]](
        LocalEviction(blockId, executorId, size, prevEvicted, onDisk))
  }

  def localEvictionDone(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    driverEndpoint.ask(LocalEvictionDone(blockId, executorId, onDisk))
  }

  def sendRDDCompTime(rddId: Int, time: Long): Unit = {
    driverEndpoint.ask(SendNoCachedRDDCompTime(rddId, time))
  }

  def sendRDDElapsedTime(srcBlock: String, dstBlock: String, clazz: String, time: Long): Unit = {
    driverEndpoint.ask(SendRDDElapsedTime(srcBlock, dstBlock, clazz, time))
  }

  def sendTaskAttempBlock(taskAttemp: Long, blockId: BlockId): Unit = {
    driverEndpoint.ask(TaskAttempBlockId(taskAttemp, blockId))
  }

  def sendRecompTime(blockId: BlockId, time: Long): Unit = {
    driverEndpoint.ask(SendRecompTime(blockId, time))
  }

  def sizePrediction(blockId: BlockId, executorId: String): Long = {
    driverEndpoint.askSync[Long](SizePrediction(blockId, executorId))
  }

  def sendSize(blockId: BlockId, executorId: String, size: Long): Unit = {
    driverEndpoint.ask(SendSize(blockId, executorId, size))
  }

  def sendSerMetric(blockId: BlockId, size: Long, cost: Long): Unit = {
    driverEndpoint.ask(WriteDisaggBlock(blockId, size, cost))
  }

  def readLocalBlock(blockId: BlockId, executorId: String, fromRemote: Boolean,
                     onDisk: Boolean, time: Long): Unit = {
    driverEndpoint.ask(ReadBlockFromLocal(blockId, executorId, fromRemote, onDisk, time))
  }

  def sendDeserMetric(blockId: BlockId, size: Long, time: Long): Unit = {
    driverEndpoint.ask(ReadDisaggBlock(blockId, size, time))
  }

  def cacheDisaggDataInMemory(blockId: BlockId, size: Long,
                              executorId: String,
                              enoughSpace: Boolean,
                              fromDisk: Boolean): Boolean = {
    driverEndpoint.askSync[Boolean](PromoteToMemory(blockId, size,
      executorId, enoughSpace, fromDisk))
  }

  private val localBlockCache = new ConcurrentHashMap[BlockId, Long].asScala

  def getLocalBlockSize(blockId: BlockId): Long = {
    val v = localBlockCache.get(blockId)

    if (v.isDefined) {
      v.get
    } else {
      val size = driverEndpoint.askSync[Long](GetLocalBlockSize(blockId))
      if (size > 0) {
        localBlockCache.putIfAbsent(blockId, size)
      }
      size
    }
  }

  def evictionFail(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    driverEndpoint.ask(EvictionFail(blockId, executorId, onDisk))
  }

  def cachingFail(blockId: BlockId, estimateSize: Long, executorId: String,
                  putDisagg: Boolean, localFull: Boolean, onDisk: Boolean): Unit = {
    driverEndpoint.ask(
      CachingFail(blockId, estimateSize, executorId, putDisagg, localFull, onDisk))
  }


  def diskCachingDone(blockId: BlockId, estimateSize: Long, executorId: String): Unit = {
    driverEndpoint.ask(
      DiskCachingDone(blockId, estimateSize, executorId))
  }

  def cachingDone(blockId: BlockId, estimateSize: Long, executorId: String,
                  onDisk: Boolean): Unit = {
    driverEndpoint.ask(
      CachingDone(blockId, estimateSize, executorId, onDisk))
  }

  def cachingDecision(blockId: BlockId, estimateSize: Long,
                      executorId: String, putDisagg: Boolean,
                      localFull: Boolean, onDisk: Boolean, promote: Boolean): Boolean = {

    // val taskContext = TaskContext.get()
    // val taskId = s"${taskContext.stageId()}-" +
    //  s"${taskContext.partitionId()}-${taskContext.attemptNumber()}"

    if (putDisagg) {
      // Diable disagg
      false
    } else {
      val attemp = TaskContext.get().taskAttemptId()
      driverEndpoint.askSync[Boolean](
        StoreBlockOrNot(blockId, estimateSize, executorId, putDisagg,
          localFull, onDisk, promote, attemp))
    }
  }

  def read(blockId: BlockId, executorId: String) : Boolean = {
    return false
        /*
    val result = driverEndpoint.askSync[Int](FileRead(blockId, executorId))

    logInfo(s"Read logging ... $blockId, result: $result")

    if (result == 1) {
      true
    } else if (result == 2) {
      // retry... the block is being written
      Thread.sleep(500)
      logInfo(s"Try read again... $blockId, executor $executorId")
      read(blockId, executorId)
    } else if (result == 0) {
      false
    } else {
      throw new RuntimeException(s"Invalid read value $result for reading $blockId")
    }
    */
  }

  def readUnlock(blockId: BlockId, executorId: String) : Unit = {
    driverEndpoint.ask[Unit](FileReadUnlock(blockId, executorId))
  }

  def createFile(blockId: BlockId, executorId: String) : CrailFile = {
    val path = getPath(blockId)

    logInfo("jy: disagg: getting result for create file " + blockId.name)

    val fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().asFile()
    logInfo("jy: disagg: fresh file, writing " + blockId.name)
    fileInfo
  }

  def getFile(blockId: BlockId): CrailFile = {
    val path = getPath(blockId)
    fs.lookup(path).get().asFile()
  }

  def remove(blockId: BlockId, executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](FileRemoved(blockId, executorId, remove = true))
    /*
    if (blockExists(blockId)) {
      val path = getPath(blockId)
      fs.delete(path, false)
      logInfo(s"jy: Removed block $blockId from disagg")

      logInfo(s"Removed block $blockId lookup ${fs.lookup(path).get()}")

      true
    } else {
      logInfo(s"Block $blockId is already removed from disagg")
      false
    }
    */
  }

  def blockExists(blockId: BlockId, executorId: String): Boolean = {
    try {
      var result = driverEndpoint.askSync[Int](Contains(blockId, executorId))
      while (result == 2) {
        logInfo(s"blockExist check again $blockId, executorId: $executorId")
        Thread.sleep(1000)
        result = driverEndpoint.askSync[Int](Contains(blockId, executorId))
      }
      result == 1
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }
  }

  def getSize(blockId: BlockId, executorId: String): Long = {
    val size = driverEndpoint.askSync[Long](GetSize(blockId, executorId))

    if (size == -1) {
      logInfo(s"Get size again !! $blockId, $executorId")
      Thread.sleep(500)
    }

    logInfo(s"Disagg get block size $blockId, size $size")
    size
  }

  def writeEnd(blockId: BlockId, executorId: String, size: Long): Unit = {
    logInfo(s"jy: Writing end $blockId from disagg")
    driverEndpoint.ask(FileWriteEnd(blockId, executorId, size))
  }
}
