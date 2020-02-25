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

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}

import scala.collection.mutable


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class RDDCostBasedEvictionEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
   extends DisaggBlockManagerEndpoint(
    rpcEnv, isLocal, conf, listenerBus, blockManagerMaster, thresholdMB) {
  logInfo("RDDCostBasedEvictionEndpoint up")

  override def taskStartedCall(taskId: String): Unit = {
    rddJobDag match {
      case None =>
        // do nothing
      case Some(dag) =>
        dag.taskStarted(taskId)
    }
  }

  override def stageCompletedCall(stageId: Int): Unit = {
    rddJobDag match {
      case None =>
      // do nothing
      case Some(dag) =>
        dag.removeCompletedStageNode(stageId)
    }
  }

  override def stageSubmittedCall(stageId: Int): Unit = {
    logInfo(s"Stage submitted ${stageId}")
    rddJobDag match {
      case None =>
      // do nothing
      case Some(dag) =>
        dag.stageSubmitted(stageId)
    }
  }

  override def cachingDecision(blockId: BlockId, estimateSize: Long, taskId: String): Boolean = {
    val prevTime = prevDiscardTime.get()

    rddJobDag.get.setStoredBlocksCreatedTime(blockId)
    rddJobDag.get.setBlockCreatedTime(blockId)

    val storingCost =
      rddJobDag.get.calculateCostToBeStored(blockId, System.currentTimeMillis()).cost

    // logInfo(s"Request $blockId, size $estimateSize 222")

    if (storingCost < 2000) {
      // the cost due to discarding >  cost to store
      // we won't store it
      logInfo(s"Discarding $blockId, discardingCost: $storingCost")

      return false
    }

    val rddId = blockId.asRDDId.get.rddId

    synchronized {
      if (disaggBlockInfo.contains(blockId)) {
        return false
      }

      // If we have enough space in disagg memory, cache it

     // if (totalSize.get() + estimateSize < threshold) {
      logInfo(s"Storing $blockId, cost: $storingCost, " +
        s"size $estimateSize / $totalSize, threshold: $threshold")

      blocksSizeToBeCreated.put(blockId, estimateSize)
      totalSize.addAndGet(estimateSize)
      rddJobDag.get.storingBlock(blockId, estimateSize)

      return true
      // }

      // Else, select a victim to evict

      /*
      var totalCost = 0L
      var totalDiscardSize = 0L

      val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
        new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

      if (prevDiscardTime.compareAndSet(prevTime, System.currentTimeMillis())) {

        rddJobDag.get.sortedBlockCost match {
          case None =>
            None
          case Some(l) =>
            val iterator = l.iterator

            val removalSize = Math.max(estimateSize,
              totalSize.get() + estimateSize - threshold)

            val currTime = System.currentTimeMillis()

            if (removalSize <= estimateSize) {
              while (iterator.hasNext && totalDiscardSize < removalSize) {
                val (bid, discardBlockCost) = iterator.next()
                val discardCost = discardBlockCost.cost
                disaggBlockInfo.get(bid) match {
                  case None =>
                  // do nothing
                  case Some(blockInfo) =>
                    // timeToRemove: prevent immediate eviction of the cached block
                    if (discardCost <= 0 && timeToRemove(blockInfo.createdTime, currTime)
                      && !recentlyRemoved.contains(bid) && !bid.asRDDId.get.rddId.equals(rddId)) {
                      // GC 0 cost blocks

                      totalDiscardSize += blockInfo.size
                      removeBlocks.append((bid, blockInfo))
                      logInfo(s"Try to remove: Cost: $totalCost/$storingCost, " +
                        s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                    } else if (totalCost + discardCost < storingCost
                      && timeToRemove(blockInfo.createdTime, currTime)
                      && !recentlyRemoved.contains(bid)) {
                      totalCost += discardCost
                      totalDiscardSize += blockInfo.size
                      removeBlocks.append((bid, blockInfo))
                      logInfo(s"Try to remove: Cost: $totalCost/$storingCost, " +
                        s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                    }
                }
              }
            } else {
              // remove blocks til threshold without considering cost
              // for hard threshold
              while (iterator.hasNext && totalDiscardSize < removalSize) {
                val (bid, discardBlockCost) = iterator.next()
                val discardCost = discardBlockCost.cost

                disaggBlockInfo.get(bid) match {
                  case None =>
                  // do nothing
                  case Some(blockInfo) =>
                    if (discardCost <= 0 && timeToRemove(blockInfo.createdTime, currTime)
                      && !recentlyRemoved.contains(bid) && !bid.asRDDId.get.rddId.equals(rddId)) {

                      // evict the victim
                      totalDiscardSize += blockInfo.size
                      removeBlocks.append((bid, blockInfo))
                      logInfo(s"Try to remove: Cost: $totalCost/$storingCost, " +
                        s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                    } else if (timeToRemove(blockInfo.createdTime, currTime)
                      && !recentlyRemoved.contains(bid)) {
                      totalCost += discardCost
                      totalDiscardSize += blockInfo.size
                      removeBlocks.append((bid, blockInfo))
                      logInfo(s"Try to remove: Cost: $totalCost/$storingCost, " +
                        s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                    }
                }
              }
            }
        }
      }


      if (totalDiscardSize < estimateSize) {
        // the cost due to discarding >  cost to store
        // we won't store it
        logInfo(s"Discarding $blockId, discardingCost: $totalCost, " +
          s"discardingSize: $totalDiscardSize/$estimateSize")
        rddJobDag.get.setStoredBlocksCreatedTime(blockId)

        false
      } else {

        evictBlocks(removeBlocks)
        removeBlocks.foreach { t =>
          rddJobDag.get.removingBlock(t._1)
        }

        logInfo(s"Storing $blockId, size $estimateSize / $totalSize, threshold: $threshold")
        blocksSizeToBeCreated.put(blockId, estimateSize)
        totalSize.addAndGet(estimateSize)
        rddJobDag.get.storingBlock(blockId, estimateSize)

        true
      }
      */
    }
  }

  override def evictBlocksToIncreaseBenefit(
                totalCompReduction: Long, totalSize: Long): Unit = synchronized {

    logInfo("Call evictBlocksToIncrease benefit...")
    // do sth !!
    val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
      new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

    rddJobDag match {
      case None =>
      case Some(jobDag) =>
        jobDag.sortedBlockByBenefit match {
          case None =>
          case Some(sortedBlocks) =>
            val prevBenefit = totalCompReduction.toDouble / totalSize
            val iterator = sortedBlocks.iterator

            while (iterator.hasNext) {
              val tuple = iterator.next()
              val blockId = tuple._1
              val benefit = tuple._2

              val adjustBenefit = (totalCompReduction -
                benefit.totalReduction).toDouble /
                (totalSize - benefit.totalSize)

              if (adjustBenefit >= prevBenefit) {
                logInfo(s"Remove block for " +
                  s"adjusted benefit: $adjustBenefit/$prevBenefit, " +
                  s"for block $blockId, ${benefit.totalReduction}, ${benefit.totalSize}")

                disaggBlockInfo.get(blockId) match {
                  case Some(blockInfo) =>
                    removeBlocks.append((blockId, blockInfo))
                  case None =>
                }
              }
            }
        }
    }

    evictBlocks(removeBlocks)
    removeBlocks.foreach { t =>
      rddJobDag.get.removingBlock(t._1)
    }
  }

  override def fileWriteEndCall(blockId: BlockId, size: Long): Unit = {
    rddJobDag.get.storingBlock(blockId, size)
  }

  override def fileRemovedCall(blockInfo: CrailBlockInfo): Unit = {
    rddJobDag.get.removingBlock(blockInfo.bid)
  }

  override def fileCreatedCall(blockInfo: CrailBlockInfo): Unit = {
    // do nothing
  }

  override def fileReadCall(blockInfo: CrailBlockInfo): Unit = {
    // do nothing
  }
}
