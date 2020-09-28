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

import alluxio.AlluxioURI
import alluxio.client.file.{FileInStream, FileOutStream, FileSystem}
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.decorateAsScala._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._

private[spark] class DisaggBlockManager(
      var driverEndpoint: RpcEndpointRef,
      conf: SparkConf) extends Logging {

  def discardBlocksIfNecessary(estimateSize: Long): Unit = {
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

  def localEviction(blockId: Option[BlockId], executorId: String, size: Long,
                    prevEvicted: Set[BlockId]): List[BlockId] = {
    driverEndpoint.askSync[List[BlockId]](LocalEviction(blockId, executorId, size, prevEvicted))
  }

  def localEvictionDone(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask(LocalEvictionDone(blockId, executorId))
  }

  def sendRDDCompTime(rddId: Int, time: Long): Unit = {
    driverEndpoint.ask(SendNoCachedRDDCompTime(rddId, time))
  }

  def sendRecompTime(blockId: BlockId, time: Long): Unit = {
    driverEndpoint.ask(SendRecompTime(blockId, time))
  }

  def sendSerMetric(blockId: BlockId, size: Long, cost: Long): Unit = {
    driverEndpoint.ask(WriteDisaggBlock(blockId, cost))
  }

  def readLocalBlock(blockId: BlockId, executorId: String, fromRemote: Boolean): Unit = {
    driverEndpoint.ask(ReadBlockFromLocal(blockId, executorId, fromRemote))
  }

  def sendDeserMetric(blockId: BlockId, time: Long): Unit = {
    driverEndpoint.ask(ReadDisaggBlock(blockId, time))
  }

  def cacheDisaggDataInMemory(blockId: BlockId, size: Long,
                              executorId: String,
                              enoughSpace: Boolean): Boolean = {
    driverEndpoint.askSync[Boolean](CacheDisaggInMemory(blockId, size, executorId, enoughSpace))
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

  def evictionFail(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask(EvictionFail(blockId, executorId))
  }

  def cachingFail(blockId: BlockId, estimateSize: Long, executorId: String,
                  putDisagg: Boolean, localFull: Boolean): Unit = {
    driverEndpoint.ask(
      CachingFail(blockId, estimateSize, executorId, putDisagg, localFull))
  }

  def readLock(blockId: BlockId, executorId: String): Boolean = {
    val result = driverEndpoint.askSync[Int](FileRead(blockId, executorId))

    logInfo(s"Read logging ... $blockId, result: $result")

    if (result == 1) {
      true
    } else if (result == 2) {
      // retry... the block is being written
      Thread.sleep(500)
      logInfo(s"Try read again... $blockId, executor $executorId")
      readLock(blockId, executorId)
    } else if (result == 0) {
      false
    } else {
      throw new RuntimeException(s"Invalid read value $result for reading $blockId")
    }
  }

  def readUnlock(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask[Unit](FileReadUnlock(blockId, executorId))
  }


  def writeLock(blockId: BlockId, executorId: String): Int = {
    driverEndpoint.askSync[Int](FileWrite(blockId, executorId))
  }

  def createFileOutputStream(blockId: BlockId): FileOutStream = {
    val fs = FileSystem.Factory.get()
    val path = new AlluxioURI("/" + blockId)
    try {
      fs.createFile(path)
    } catch {
      case e: IOException => e.printStackTrace()
        throw e
    }
  }

  def getFileInputStream(blockId: BlockId, executorId: String): FileInStream = {
    val fs = FileSystem.Factory.get()
    val path = new AlluxioURI("/" + blockId)
    try {
      if (fs.exists(path) && getSize(blockId, executorId) > 0L) {
        fs.openFile(path)
      } else {
        null
      }
    } catch {
      case e: IOException => e.printStackTrace()
        throw e
    }
  }

  def remove(blockId: BlockId, executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](FileRemoved(blockId, executorId, remove = true))
  }

  def blockExists(blockId: BlockId, executorId: String): Boolean = {
    try {
      val result = driverEndpoint.askSync[Int](Contains(blockId, executorId))
      if (result == 2) {
        logInfo(s"blockExist check again $blockId, executorId: $executorId")
        // Thread.sleep(1000)
        // result = driverEndpoint.askSync[Int](Contains(blockId, executorId))
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
    logInfo(s"Writing end $blockId from disagg")
    driverEndpoint.ask(FileWriteEnd(blockId, executorId, size))
  }
}

private[spark] object DisaggBlockManager {
  val DRIVER_ENDPOINT_NAME = "DisaggBlockManager"
}
