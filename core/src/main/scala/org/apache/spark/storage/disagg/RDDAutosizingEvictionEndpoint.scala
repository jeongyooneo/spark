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
import org.apache.spark.storage.disagg.RDDJobDag.BlockCost
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class RDDAutosizingEvictionEndpoint(
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
                taskId: String, executorId: String,
                putDisagg: Boolean): Boolean = synchronized {
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
      rddJobDag.get.storingBlock(blockId)

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

  class HistoInfo(val percentage: Double,
                  val index: Int,
                  val compRatio: Double,
                  val sizeRatio: Double) {

    override def toString: String = {
      s"($percentage, $compRatio, $sizeRatio)"
    }

    def sizeCompRatio: Double = {
      sizeRatio / compRatio
    }
  }

  private def calculateHistogram(blocks: mutable.ListBuffer[(BlockId, BlockCost)]) = {
    val percents = List(0.1, 0.2, 0.4, 0.8, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0)
    val indices = percents.map(percent => (blocks.size * 0.01 * percent).toInt)

    var compSum = 0L
    var sizeSum = 0L
    val histogram: mutable.ListBuffer[HistoInfo] =
      new ListBuffer[HistoInfo]

    val totalCost = blocks.map(block => block._2.cost).sum
    val totalSize = blocks.map(block => disaggBlockInfo.get(block._1) match {
      case None => 0L
      case Some(info) => info.size
    }).sum

    var percentIndex = 0
    for (i <- blocks.indices) {

      val s = disaggBlockInfo.get(blocks(i)._1) match {
        case None => 0L
        case Some(info) => info.size
      }

      compSum += blocks(i)._2.cost
      sizeSum += s

      // find index
      val percentage = 100 * compSum.toDouble / Math.max(1, totalCost)
      var toAdd = false
      while (percentIndex < percents.size && percentage >= percents(percentIndex)) {
        percentIndex += 1
        toAdd = true
      }

      if (toAdd && percentage <= percents.last
        // prevent zero division
        && totalSize > 500 && totalCost > 10) {
        histogram.append(new HistoInfo(percentage, i,
          compSum.toDouble / totalCost, sizeSum.toDouble / totalSize))
      }
    }

    (histogram, totalCost / Math.max(1, blocks.size))
  }

  private def findMaxSizeCompRatio(
                histogram: ListBuffer[HistoInfo]) = {
    var maxVal: HistoInfo = new HistoInfo(0, 0, 1, 0)

    histogram.foreach(x => {
      if (maxVal.sizeCompRatio < x.sizeCompRatio) {
        maxVal = x
      }
    })

    maxVal
  }

  private def removePercent(blocks: mutable.ListBuffer[(BlockId, BlockCost)],
                            index: Int,
                            avgCost: Long) = {
    val currTime = System.currentTimeMillis()
    val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
      new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

    for (i <- 0  until index) {
      disaggBlockInfo.get(blocks(i)._1) match {
        case None =>
        case Some(blockInfo) =>
          if (timeToRemove(blockInfo.createdTime, currTime) &&
                blocks(i)._2.cost < avgCost) {
            logInfo(s"Remove block for histogram " +
              s"for block ${blockInfo.bid}, ${blocks(i)._2.cost}, ${blockInfo.size}, " +
              s"avgCost: $avgCost")
            removeBlocks.append((blockInfo.bid, blockInfo))
          }
      }
    }

    removeBlocks
  }


  var prevEvictTime = System.currentTimeMillis()

  override def evictBlocksToIncreaseBenefit(
                totalCompReduction: Long, totalSize: Long): Unit = synchronized {

    // compute histogram
    // max: 5% computation


    logInfo("Call evictBlocksToIncrease benefit...")
    // do sth !!
    val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
      new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

    val currTime = System.currentTimeMillis()

    rddJobDag match {
      case None =>
      case Some(jobDag) =>
        jobDag.sortedBlockCost match {
          case None =>
          case Some(sortedBlocks) =>

            val (histogram, avgCost) = calculateHistogram(sortedBlocks)
            val maxSizeCompRatio = findMaxSizeCompRatio(histogram)
            logInfo(s"histogram: $histogram\n maxSizeCompRatio for histogram: $maxSizeCompRatio")

            // sizeReduction / compReduction >= 2
            if (maxSizeCompRatio.sizeCompRatio >= compDiscardRatio &&
            System.currentTimeMillis() - prevEvictTime >= 6000) {
              prevEvictTime = System.currentTimeMillis()
              val percent = maxSizeCompRatio.percentage
              logInfo(s"Start to evict ${percent/maxSizeCompRatio.index/sortedBlocks.size}" +
                s" blocks for histogram... ${maxSizeCompRatio}")
              removeBlocks.appendAll(removePercent(sortedBlocks, maxSizeCompRatio.index, avgCost))
            }

          /*
        val prevBenefit = totalCompReduction.toDouble / totalSize
        val iterator = sortedBlocks.iterator

        var rmComp: Long = 0L
        var rmSize: Long = 0L



        while (iterator.hasNext) {
          val tuple = iterator.next()
          val blockId = tuple._1
          val benefit = tuple._2

          disaggBlockInfo.get(blockId) match {
            case Some(blockInfo) =>
              if (timeToRemove(blockInfo.createdTime, currTime) &&
                benefit.totalReduction <= TimeUnit.SECONDS.toMillis(2)) {
                logInfo(s"Remove block for decreasing comp benefit " +
                  s"for block $blockId, ${benefit.totalReduction}, ${benefit.totalSize}")
                removeBlocks.append((blockId, blockInfo))
              }
            case None =>
          }

          rmComp += benefit.totalReduction
          rmSize += benefit.totalSize
          val adjustBenefit = (totalCompReduction -
            rmComp).toDouble /
            (totalSize - rmSize)

          if (adjustBenefit >= prevBenefit) {
            logInfo(s"Remove block for " +
              s"adjusted benefit: $adjustBenefit/$prevBenefit, " +
              s"for block $blockId, ${benefit.totalReduction}, ${benefit.totalSize}")

            disaggBlockInfo.get(blockId) match {
              case Some(blockInfo) =>
                if (timeToRemove(blockInfo.createdTime, currTime)) {
                  removeBlocks.append((blockId, blockInfo))
                }
              case None =>
            }
          } else {
            rmComp -= benefit.totalReduction
            rmComp -= benefit.totalSize
          }
          */
            }
        }

    evictBlocks(removeBlocks)
    removeBlocks.foreach { t =>
      rddJobDag.get.removingBlock(t._1)
    }
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
}
