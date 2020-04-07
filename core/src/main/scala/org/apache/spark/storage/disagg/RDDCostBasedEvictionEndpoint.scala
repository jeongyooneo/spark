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

import scala.collection.{Set, mutable}


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

  override def removeRddFromDisagg(rdds: Set[Int]): Unit = {
    // Remove cost 0
    val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
      new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

    rddJobDag.get.sortedBlockCost match {
      case None =>
        None
      case Some(l) =>
        val iterator = l.iterator

        while (iterator.hasNext) {
          val (bid, discardBlockCost) = iterator.next()
          disaggBlockInfo.get(bid) match {
            case None =>
            // do nothing
            case Some(blockInfo) =>
              if (rdds.contains(bid.asRDDId.get.rddId) && !recentlyRemoved.contains(bid)) {
                logInfo(s"Discarding zero cost $bid")
                removeBlocks.append((bid, blockInfo))
              }
          }
        }
    }

    evictBlocks(removeBlocks)
    removeBlocks.foreach { t =>
      rddJobDag.get.removingBlock(t._1)
    }

  }

  override def cachingDecision(blockId: BlockId, estimateSize: Long,
                               executorId: String,
                               putDisagg: Boolean): Boolean = {
    /*
    val r = scala.util.Random
    if (taskId.contains("rdd_2_")) {
      true
    } else {
      false
    }
    */
    val prevTime = prevDiscardTime.get()

    rddJobDag.get.setStoredBlocksCreatedTime(blockId)
    rddJobDag.get.setBlockCreatedTime(blockId)

    val storingCost =
      rddJobDag.get.calculateCostToBeStored(blockId, System.currentTimeMillis()).cost

    if (storingCost < 2000) {
      // the cost due to discarding >  cost to store
      // we won't store it
      logInfo(s"Discarding $blockId, discardingCost: $storingCost")

      return false
    }

    val rddId = blockId.asRDDId.get.rddId

    val estimateBlockSize = DisaggUtils.calculateDisaggBlockSize(estimateSize)

    synchronized {
      if (disaggBlockInfo.contains(blockId)) {
        return false
      }

      val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
        new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

      // If we have enough space in disagg memory, cache it
      if (totalSize.get() + estimateBlockSize < threshold) {
        logInfo(s"Storing $blockId, cost: $storingCost, " +
          s"size $estimateBlockSize / $totalSize, threshold: $threshold")

        blocksSizeToBeCreated.put(blockId, estimateBlockSize)
        totalSize.addAndGet(estimateBlockSize)
        rddJobDag.get.storingBlock(blockId)

        return true
      }

      // Else, select a victim to evict
      var totalCost = 0L
      var totalDiscardSize = 0L
        rddJobDag.get.sortedBlockCost match {
          case None =>
            None
          case Some(l) =>
            val iterator = l.iterator

            val removalSize = Math.max(estimateBlockSize,
              totalSize.get() + estimateBlockSize - threshold + 1 * (1000 * 1000))

            val currTime = System.currentTimeMillis()

            while (iterator.hasNext) {
              val (bid, discardBlockCost) = iterator.next()
              val discardCost = discardBlockCost.cost

              disaggBlockInfo.get(bid) match {
                case None =>
                // do nothing
                case Some(blockInfo) =>
                  if (timeToRemove(blockInfo.createdTime, currTime)
                    && !recentlyRemoved.contains(bid) && totalDiscardSize < removalSize) {
                    totalCost += discardCost
                    totalDiscardSize += blockInfo.getActualBlockSize
                    removeBlocks.append((bid, blockInfo))
                    logInfo(s"Try to remove: Cost: $totalCost/$storingCost, " +
                      s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                  }
              }
            }
        }


      if (totalDiscardSize < estimateBlockSize) {
        // the cost due to discarding >  cost to store
        // we won't store it
        logInfo(s"Discarding $blockId, discardingCost: $totalCost, " +
          s"discardingSize: $totalDiscardSize/$estimateBlockSize")
        rddJobDag.get.setStoredBlocksCreatedTime(blockId)
        false
      } else if (totalCost < storingCost) {
        // the cost due to discarding >  cost to store
        // we won't store it
        logInfo(s"Discarding $blockId because the discarding " +
          s"cost is low, discardingCost: $totalCost, " +
          s"discardingSize: $totalDiscardSize/$estimateBlockSize")
        rddJobDag.get.setStoredBlocksCreatedTime(blockId)
        false
      } else {

        evictBlocks(removeBlocks)
        removeBlocks.foreach { t =>
          rddJobDag.get.removingBlock(t._1)
        }

        logInfo(s"Storing $blockId, size $estimateBlockSize / $totalSize, threshold: $threshold")
        blocksSizeToBeCreated.put(blockId, estimateBlockSize)
        totalSize.addAndGet(estimateBlockSize)
        rddJobDag.get.storingBlock(blockId)

        true
      }
    }
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

  override def evictBlocksToIncreaseBenefit(totalCompReduction: Long, totalSize: Long): Unit = {

  }

  override def fileWriteEndCall(blockId: BlockId, size: Long): Unit = {

  }
}
