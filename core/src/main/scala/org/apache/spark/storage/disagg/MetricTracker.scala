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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable
import scala.collection.convert.decorateAsScala._

private[spark] class MetricTracker extends Logging {

  // Private values
  val localStoredBlocksMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  val localStoredBlocksHistoryMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  private val disaggStoredBlocks = new mutable.HashSet[BlockId]()

  private val disaggBlockSizeMap = new ConcurrentHashMap[BlockId, Long]()
  private val localBlockSizeMap = new ConcurrentHashMap[BlockId, Long]()
  val localBlockSizeHistoryMap = new ConcurrentHashMap[BlockId, Long]()

  // Public values
  // disagg total size
  val disaggTotalSize: AtomicLong = new AtomicLong(0)
  val blockSerCostMap = new ConcurrentHashMap[BlockId, Long]()
  val blockDeserCostMap = new ConcurrentHashMap[BlockId, Long]()
  // TODO set
  val hitInLocal = new AtomicInteger()
  // TODO set
  val missInLocal = new AtomicInteger()
  // TODO set
  val hitInDisagg = new AtomicInteger()
  // TODO set
  val missInDisagg = new AtomicInteger()

  val blockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()
  val recentlyBlockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()

  // TODO set
  val stageStartTime = new ConcurrentHashMap[Int, Long]().asScala
  // TODO set
  val taskStartTime = new ConcurrentHashMap[String, Long]().asScala
  // TODO set
  val completedStages: mutable.Set[Int] = new mutable.HashSet[Int]()

  def storedBlocks: Set[BlockId] = {
    localStoredBlocksMap.values.asScala
      .reduceOption((x, y) => x.union(y)) match {
      case None =>
        Set.empty
      case Some(set) =>
        set.union(disaggStoredBlocks).toSet
    }
  }

  def blockStored(blockId: BlockId): Boolean = {
    localStoredBlocksMap.entrySet().iterator().asScala
      .foreach {
        entry => if (entry.getValue.contains(blockId)) {
          return true
        }
      }
    disaggStoredBlocks.contains(blockId)
  }

  def getBlockSize(blockId: BlockId): Long = synchronized {
    if (localBlockSizeMap.containsKey(blockId)) {
      localBlockSizeMap.get(blockId)
    } else if (disaggBlockSizeMap.containsKey(blockId)) {
      disaggBlockSizeMap.get(blockId)
    } else if (localBlockSizeHistoryMap.containsKey(blockId)) {
      localBlockSizeHistoryMap.get(blockId)
    } else {
      logWarning(s"No block size $blockId")
      0L
    }
  }

  def getExecutorBlocksMap: Map[String, mutable.Set[BlockId]] = {
    localStoredBlocksMap.asScala.toMap
  }

  def storedBlockInLocal(blockId: BlockId): Boolean = {
    localStoredBlocksMap.asScala.foreach {
      entry => if (entry._2.contains(blockId)) {
        return true
      }
    }
    false
  }

  def containBlockInLocal(blockId: BlockId, executorId: String): Boolean = {
    localStoredBlocksHistoryMap.get(executorId).contains(blockId)
  }

  def getExecutorBlocks(executorId: String): mutable.Set[BlockId] = {
    localStoredBlocksMap.putIfAbsent(executorId, ConcurrentHashMap.newKeySet[BlockId].asScala)
    localStoredBlocksMap.get(executorId)
  }

  def removeExecutorBlocks(executorId: String): Unit = synchronized {
    localStoredBlocksMap.remove(executorId).foreach {
      blockId => {
        val size = localBlockSizeMap.remove(blockId)
        BlazeLogger.removeLocal(blockId, executorId, size)
      }
    }
  }

  def removeExecutorBlock(blockId: BlockId, executorId: String): Unit = {
    localBlockSizeMap.remove(blockId)
    localStoredBlocksMap.get(executorId).remove(blockId)
  }

  def addExecutorBlock(blockId: BlockId, executorId: String, size: Long): Unit = {
    localBlockSizeMap.put(blockId, size)
    localStoredBlocksMap.putIfAbsent(executorId, ConcurrentHashMap.newKeySet[BlockId].asScala)
    localStoredBlocksMap.get(executorId).add(blockId)
  }

  def addBlockInDisagg(blockId: BlockId, size: Long): Unit = {
    disaggBlockSizeMap.put(blockId, size)
    disaggTotalSize.addAndGet(size)
    disaggStoredBlocks.add(blockId)
  }

  def adjustDisaggBlockSize(blockId: BlockId, size: Long): Unit = {
    val prevSize = disaggBlockSizeMap.get(blockId)
    disaggBlockSizeMap.put(blockId, size)
    disaggTotalSize.addAndGet(-prevSize + size)
    logInfo(s"Adjust block size $blockId prev: $prevSize, size: $size")
  }

  def removeDisaggBlock(blockId: BlockId): Unit = {
    if (disaggBlockSizeMap.containsKey(blockId)) {
      val size = disaggBlockSizeMap.remove(blockId)
      disaggTotalSize.addAndGet(-size)
      disaggStoredBlocks.remove(blockId)
    }
  }


  def getDisaggBlocks: mutable.Set[BlockId] = {
    disaggStoredBlocks
  }

  def getTaskStartTimeFromBlockId(stageId: Int, blockId: BlockId): Option[Long] = {
    val index = blockId.name.split("_")(2).toInt

    for(i <- 0 to index) {
      val taskId = s"$stageId-$i-0"
      taskStartTime.get(taskId) match {
        case None =>
          //  do nothing
        case Some(startTime) =>
          return Some(startTime)
      }
    }

    None
  }

  def taskStarted(taskId: String): Unit = synchronized {
    logInfo(s"Handling task ${taskId} started")
    taskStartTime.putIfAbsent(taskId, System.currentTimeMillis())
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
