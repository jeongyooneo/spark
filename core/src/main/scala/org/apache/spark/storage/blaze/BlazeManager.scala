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

package org.apache.spark.storage.blaze

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.blaze.BlazeBlockManagerMessages._
import org.apache.spark.TaskContext

import scala.collection.convert.decorateAsScala._

class BlazeManager(var driverEndpoint: RpcEndpointRef) extends Logging {

  private val rddIdToCacheMap = new ConcurrentHashMap[Int, Boolean]().asScala
  private var prevUpdate = System.currentTimeMillis()
  private val expiredPeriod = 1000

  def isRDDToCache(rddId: Int): Boolean = {
    val curr = System.currentTimeMillis()
    if (curr - prevUpdate >= expiredPeriod) {
      rddIdToCacheMap.clear()
      prevUpdate = curr
    }

    if (rddIdToCacheMap.contains(rddId)) {
      rddIdToCacheMap(rddId)
    } else {
      val result = driverEndpoint.askSync[Boolean](IsRddToCache(rddId))
      rddIdToCacheMap(rddId) = result
      result
    }
  }

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
    driverEndpoint.ask(SendCompTime(rddId, time))
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
    driverEndpoint.ask(SendSerTime(blockId, size, cost))
  }

  def readLocalBlock(blockId: BlockId, stageId: String, fromRemote: Boolean,
                     onDisk: Boolean, time: Long): Unit = {
    driverEndpoint.ask(ReadBlockFromLocal(blockId, stageId, fromRemote, onDisk, time))
  }

  def promoteToMemory(blockId: BlockId, size: Long,
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
                  localFull: Boolean, onDisk: Boolean): Unit = {
    driverEndpoint.ask(
      CachingFail(blockId, estimateSize, executorId, localFull, onDisk))
  }


  def diskCachingDone(blockId: BlockId, estimateSize: Long, executorId: String): Unit = {
    driverEndpoint.ask(
      DiskCachingDone(blockId, estimateSize, executorId))
  }

  def cachingDone(blockId: BlockId, estimateSize: Long, stageId: String,
                  onDisk: Boolean): Unit = {
    driverEndpoint.ask(
      CachingDone(blockId, estimateSize, stageId, onDisk))
  }

  def cachingDecision(blockId: BlockId, estimateSize: Long,
                      executorId: String,
                      localFull: Boolean, onDisk: Boolean, promote: Boolean): Boolean = {
    val attempt = if (TaskContext.get() != null) {
      TaskContext.get().taskAttemptId()
    } else {
      0
    }

    driverEndpoint.askSync[Boolean](
      StoreBlockOrNot(blockId, estimateSize, executorId,
        localFull, onDisk, promote, attempt))
  }
}
