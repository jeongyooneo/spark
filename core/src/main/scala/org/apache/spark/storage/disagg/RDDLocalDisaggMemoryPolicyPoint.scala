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
import scala.collection.mutable.ListBuffer


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class RDDLocalDisaggMemoryPolicyPoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
   extends DisaggBlockManagerEndpoint(
    rpcEnv, isLocal, conf, listenerBus, blockManagerMaster, thresholdMB) {
  logInfo("RDDCostBasedEvictionEndpoint up")

  val compDiscardRatio = conf.getDouble("spark.disagg.autosizing.comp", defaultValue = 5.0)

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

  override def cachingDecision(
                blockId: BlockId, estimateSize: Long,
                executorId: String,
                putDisagg: Boolean): Boolean = synchronized {

    if (!putDisagg) {
      val prevTime = prevDiscardTime.get()

      rddJobDag.get.setStoredBlocksCreatedTime(blockId)
      rddJobDag.get.setBlockCreatedTime(blockId)

      // logInfo(s"Request $blockId, size $estimateSize 222")

      val rddId = blockId.asRDDId.get.rddId

      // if (totalSize.get() + estimateSize < threshold) {
      logInfo(s"Storing $blockId, " +
        s"size $estimateSize / $totalSize into $executorId")
      rddJobDag.get.storingBlock(blockId)
      true
    } else {
      disaggDecision(blockId, estimateSize, executorId, putDisagg)
    }
  }

  val recentlyEvictFailBlocks: mutable.Map[BlockId, Long] =
    new mutable.HashMap[BlockId, Long]().withDefaultValue(0L)

  override def localEvictionFail(blockId: BlockId, executorId: String, size: Long): Unit = {
    recentlyEvictFailBlocks.put(blockId, System.currentTimeMillis())
  }

  override def localEvictionDone(blockId: BlockId): Unit = {

  }

  override def localEviction(blockId: Option[BlockId],
                             executorId: String, evictionSize: Long): List[BlockId] = {

    val evictionList: mutable.ListBuffer[BlockId] = new ListBuffer[BlockId]

    rddJobDag.get.sortedBlockCost match {
      case None =>
      case Some(l) =>

        val blockManagerId = blockManagerMaster.executorBlockManagerMap.get(executorId).get
        val blockManagerInfo = blockManagerMaster.blockManagerInfo(blockManagerId)

        val currTime = System.currentTimeMillis()
        var sizeSum = 0L
        l.foreach {
          pair =>
            val bid = pair._1
            val cost = pair._2
            if (blockManagerInfo.blocks.contains(bid)
            && blockManagerInfo.blocks.get(bid).get.disaggSize == 0) {
              val elapsed = currTime - recentlyEvictFailBlocks.getOrElse(bid, 0L)
              if (elapsed > 10000) {
                recentlyEvictFailBlocks.remove(bid)

                sizeSum += blockManagerInfo.blocks(bid).memSize
                evictionList.append(bid)

                if (sizeSum > evictionSize && cost.cost > 0) {
                  logInfo(s"LocalDecision] Evict blocks $evictionList " +
                    s"from executor $executorId, size $evictionSize, existing blocks")
                  return evictionList.toList
                }
              }
            }
        }
    }

    List.empty
    // throw new RuntimeException(s"Eviction is not performed in $executorId, block:$blockId " +
    //  s".. size: $evictionSize, " +
    //  s"${executorBlockMap.get(executorId)}")
  }

  var prevEvictTime = System.currentTimeMillis()

  override def evictBlocksToIncreaseBenefit(
                totalCompReduction: Long, totalSize: Long): Unit = {
  }

  override def fileWriteEndCall(blockId: BlockId, size: Long): Unit = {
    // rddJobDag.get.storingBlock(blockId, size)
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

  private def disaggDecision(blockId: BlockId, estimateSize: Long,
                             executorId: String,
                             putDisagg: Boolean): Boolean = {
    val prevTime = prevDiscardTime.get()


    rddJobDag.get.setStoredBlocksCreatedTime(blockId)
    rddJobDag.get.setBlockCreatedTime(blockId)

    val storingCost =
      rddJobDag.get.calculateCostToBeStored(blockId, System.currentTimeMillis()).cost

    /*
    if (storingCost < 2000) {
      // the cost due to discarding >  cost to store
      // we won't store it

      if (blockManagerMaster.getLocations(blockId).isEmpty) {
        rddJobDag.get.removingBlock(blockId)
      }

      logInfo(s"EVICT\t$blockId\t$storingCost")
      return false
    }
    */


    val estimateBlockSize = DisaggUtils.calculateDisaggBlockSize(estimateSize)
    val rddId = blockId.asRDDId.get.rddId

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

      val discardLog: mutable.StringBuilder = new mutable.StringBuilder()

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
                  && !recentlyRemoved.contains(bid) && totalDiscardSize < removalSize
                  && discardCost < storingCost) {
                  totalCost += discardCost
                  totalDiscardSize += blockInfo.getActualBlockSize
                  removeBlocks.append((bid, blockInfo))
                  discardLog.append(s"EVICT\t$blockId\t$storingCost\n")
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


        if (blockManagerMaster.getLocations(blockId).isEmpty) {
          rddJobDag.get.removingBlock(blockId)
        }

        logInfo(s"EVICT\t$blockId\t$storingCost")
        false
      } else if (removeBlocks.isEmpty) {
        // the cost due to discarding >  cost to store
        // we won't store it
        logInfo(s"Discarding $blockId because the discarding " +
          s"block is empty, discardingCost: $totalCost, " +
          s"discardingSize: $totalDiscardSize/$estimateBlockSize")
        rddJobDag.get.setStoredBlocksCreatedTime(blockId)

        if (blockManagerMaster.getLocations(blockId).isEmpty) {
          rddJobDag.get.removingBlock(blockId)
        }

        logInfo(s"EVICT\t$blockId\t$storingCost")
        false
      } else {

        logInfo(discardLog.toString())

        evictBlocks(removeBlocks)
        removeBlocks.foreach { t =>
          rddJobDag.get.removingBlock(t._1)
        }

        logInfo(s"Storing $blockId, size $estimateBlockSize /" +
          s" $totalSize, threshold: $threshold")
        blocksSizeToBeCreated.put(blockId, estimateBlockSize)
        totalSize.addAndGet(estimateBlockSize)
        rddJobDag.get.storingBlock(blockId)

        true
      }
    }
  }

}
