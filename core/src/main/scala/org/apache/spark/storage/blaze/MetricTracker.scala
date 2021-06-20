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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable
import scala.collection.convert.decorateAsScala._

private[spark] class MetricTracker extends Logging {

  // Private values
  val localDiskStoredBlocksMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  val localDiskStoredBlocksSizeMap = new ConcurrentHashMap[BlockId, Long]()

  val localMemStoredBlocksMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  val localStoredBlocksHistoryMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()

  private val localMemBlockSizeMap = new ConcurrentHashMap[BlockId, Long]()
  val localBlockSizeHistoryMap = new ConcurrentHashMap[BlockId, Long]()

  val taskAttempBlockCount = new ConcurrentHashMap[String, AtomicInteger]()

  val blockElapsedTimeMap = new ConcurrentHashMap[String, Long]()

  val currJob = new AtomicInteger(-1)

  // Public values
  val blockSerCostMap = new ConcurrentHashMap[BlockId, Long]()
  val blockDeserCostMap = new ConcurrentHashMap[BlockId, Long]()
  // TODO: miss ratio curve
  val hitInLocal = new AtomicInteger()
  val missInLocal = new AtomicInteger()

  // cached block created time map
  val blockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()

  val recentlyBlockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()

  // TODO set
  val stageStartTime = new ConcurrentHashMap[Int, Long]().asScala
  // TODO set
  val taskStartTime = new ConcurrentHashMap[String, Long]().asScala
  val taskFinishTime = new ConcurrentHashMap[String, Long]().asScala
  // TODO set
  val completedStages: mutable.Set[Int] = new mutable.HashSet[Int]()
  val stageJobMap = new mutable.HashMap[Int, Int]()

  def blockStored(blockId: BlockId): Boolean = {
    // blockStoredMap.containsKey(blockId)
    localMemStoredBlocksMap.entrySet().iterator().asScala
      .foreach {
        entry => if (entry.getValue.contains(blockId)) {
          return true
        }
      }
    localDiskStoredBlocksMap.entrySet().iterator().asScala
      .foreach {
        entry => if (entry.getValue.contains(blockId)) {
          return true
        }
      }

    false
  }

  val sizePredictionMap = new ConcurrentHashMap[BlockId, Long]()

  def getBlockSize(blockId: BlockId): Long = synchronized {
    if (localMemBlockSizeMap.containsKey(blockId)) {
      localMemBlockSizeMap.get(blockId)
    } else if (localBlockSizeHistoryMap.containsKey(blockId)) {
      localBlockSizeHistoryMap.get(blockId)
    } else if (localDiskStoredBlocksSizeMap.containsKey(blockId)) {
      localDiskStoredBlocksSizeMap.get(blockId)
    } else if (sizePredictionMap.containsKey(blockId)) {
      sizePredictionMap.get(blockId)
    } else {
      logWarning(s"No block size $blockId")
      0L
    }
  }

  def getExecutorLocalDiskBlocksMap: Map[String, mutable.Set[BlockId]] = {
    localDiskStoredBlocksMap.asScala.toMap
  }

  def getExecutorLocalMemoryBlocksMap: Map[String, mutable.Set[BlockId]] = {
    localMemStoredBlocksMap.asScala.toMap
  }

  def storedBlockInLocalMemory(blockId: BlockId): Boolean = {
    localMemStoredBlocksMap.asScala.foreach {
      entry => if (entry._2.contains(blockId)) {
        return true
      }
    }
    false
  }

  def removeExecutorBlocks(executorId: String): Unit = {
    localMemStoredBlocksMap.remove(executorId).foreach {
      blockId => {
        val size = localMemBlockSizeMap.remove(blockId)
        BlazeLogger.removeLocal(blockId, executorId, size)
      }
    }
  }

  def removeExecutorBlock(blockId: BlockId, executorId: String,
                          onDisk: Boolean): Unit = {
    if (onDisk) {
      localDiskStoredBlocksSizeMap.remove(blockId)
      localDiskStoredBlocksMap.get(executorId).remove(blockId)
    } else {
      localMemBlockSizeMap.remove(blockId)
      localMemStoredBlocksMap.get(executorId).remove(blockId)
    }
  }

  def addExecutorBlock(blockId: BlockId, executorId: String, size: Long,
                       onDisk: Boolean): Unit = {

    if (onDisk) {
      localDiskStoredBlocksSizeMap.put(blockId, size)
      localDiskStoredBlocksMap.putIfAbsent(executorId,
        ConcurrentHashMap.newKeySet[BlockId].asScala)
      localDiskStoredBlocksMap.get(executorId).add(blockId)
    } else {
      localMemBlockSizeMap.put(blockId, size)
      localMemStoredBlocksMap.putIfAbsent(executorId, ConcurrentHashMap.newKeySet[BlockId].asScala)
      localMemStoredBlocksMap.get(executorId).add(blockId)
    }
  }

  def taskStarted(taskId: String): Unit = {
    logInfo(s"Handling task ${taskId} started")
    taskStartTime.synchronized {
      taskStartTime.putIfAbsent(taskId, System.currentTimeMillis())
    }
  }

  def taskFinished(taskId: String): Unit = synchronized {
    logInfo(s"Handling task ${taskId} finished")
    taskFinishTime.putIfAbsent(taskId, System.currentTimeMillis())
  }

  // TODO: call
  def stageSubmitted(stageId: Int): Unit = {
    stageStartTime.putIfAbsent(stageId, System.currentTimeMillis())
  }

  // TODO: call
  def stageCompleted(stageId: Int): Unit = {
    completedStages.add(stageId)
  }
}
