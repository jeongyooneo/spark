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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

private[spark] class BlazeRecompAndDiskCostAnalyzer(val rddJobDag: RDDJobDag,
                                                    metricTracker: MetricTracker)
  extends CostAnalyzer(metricTracker) with Logging {

  // 10Gib per sec to byte per sec
  private val BANDWIDTH = (10 / 8.0) * 1024 * 1024 * 1024.toDouble

  private def disaggCostCalc(blockId: BlockId, size: Long, refCnt: Int): Long = {
    val serCost = if (metricTracker.blockSerCostMap.contains(blockId)) {
      metricTracker.blockSerCostMap.get(blockId)
    } else {
      size  / BANDWIDTH
    }

    val deserCost = if (metricTracker.blockDeserCostMap.contains(blockId)) {
      metricTracker.blockDeserCostMap.get(blockId)
    } else {
      size / BANDWIDTH
    }

    (serCost + deserCost * refCnt).toLong
  }

  // val writeThp = 5000.0 / (600 * 1024 * 1024)
  val writeThp = BlazeParameters.writeThp
  private val readThp = BlazeParameters.readThp

  override def compDisaggCostWithTaskAttemp(executorId: String,
                                            blockId: BlockId,
                                            taskAttemp: Long): CompDisaggCost = {
    val node = rddJobDag.getRDDNode(blockId)
    val stages = rddJobDag.getReferenceStages(blockId)
    val (recompTime, numShuffle) = rddJobDag.blockCompTime(blockId,
      metricTracker.blockCreatedTimeMap.get(blockId))

    val realStages = stages.filter(p => node.getStages.contains(p.stageId))
      // .filter(p => p.stageId != node.rootStage)

    val currentUsage = rddJobDag.getCurrentStageUsage(node, blockId, taskAttemp)

    logDebug(s"Current usage of rdd ${blockId}: ${currentUsage}, " +
      s"realStage: ${realStages.map(p => p.stageId)}")

    val containDisk = if (metricTracker
      .localDiskStoredBlocksMap.containsKey(executorId)
      && metricTracker.localDiskStoredBlocksMap.get(executorId).contains(blockId)) {
      0
    } else {
      1
    }

    // val futureUse = realStages.size.map(x => Math.pow(0.5, x.prevCached)).sum
    val futureUse = realStages.size + currentUsage
    val writeTime = (metricTracker.getBlockSize(blockId) * writeThp).toLong
    var readTime = (metricTracker.getBlockSize(blockId) * readThp).toLong

    /*
    if (metricTracker.blockElapsedTimeMap.contains(s"unroll-${blockId.name}")) {
      readTime +=  metricTracker.blockElapsedTimeMap.get(s"unroll-${blockId.name}")
    }

    if (metricTracker.blockElapsedTimeMap.contains(s"eviction-${blockId.name}")) {
      readTime +=  metricTracker.blockElapsedTimeMap.get(s"eviction-${blockId.name}")
    }
    */

    val recomp = if (containDisk == 0) {
      readTime * futureUse
    } else {
      recompTime * futureUse
    }

    val c = new CompDisaggCost(blockId,
      Math.min(recomp, writeTime * containDisk + readTime * futureUse),
      (writeTime * containDisk + readTime * futureUse).toLong,
      (recomp).toLong,
      futureUse,
      numShuffle,
      containDisk == 0)

    // realStages.size * recompTime)
    // logInfo(s"CompDisaggCost $blockId, " +
    //  s"refStages: ${stages.map(f => f.stageId)}, time: $recompTime")
    c.setStageInfo(realStages, recompTime)
    c
  }

  override def compDisaggCost(executorId: String,
                              blockId: BlockId): CompDisaggCost = {
    val node = rddJobDag.getRDDNode(blockId)
    val stages = rddJobDag.getReferenceStages(blockId)
    val (recompTime, numShuffle) = rddJobDag.blockCompTime(blockId,
     metricTracker.blockCreatedTimeMap.get(blockId))

    val realStages = stages.filter(p => node.getStages.contains(p.stageId))

     val containDisk = if (metricTracker
      .localDiskStoredBlocksMap.containsKey(executorId)
       && metricTracker.localDiskStoredBlocksMap.get(executorId).contains(blockId)) {
       0
     } else {
       1
     }

    // val futureUse = realStages.size.map(x => Math.pow(0.5, x.prevCached)).sum
    val futureUse = realStages.size
    val writeTime = (metricTracker.getBlockSize(blockId) * writeThp).toLong
    var readTime = (metricTracker.getBlockSize(blockId) * readThp).toLong

    /*
    if (metricTracker.blockElapsedTimeMap.contains(s"unroll-${blockId.name}")) {
      readTime +=  metricTracker.blockElapsedTimeMap.get(s"unroll-${blockId.name}")
    }

    if (metricTracker.blockElapsedTimeMap.contains(s"eviction-${blockId.name}")) {
      readTime +=  metricTracker.blockElapsedTimeMap.get(s"eviction-${blockId.name}")
    }
    */

    val recomp = if (containDisk == 0) {
      readTime * futureUse
    } else {
      recompTime * futureUse
    }

    val c = new CompDisaggCost(blockId,
      Math.min(recomp, writeTime * containDisk + readTime * futureUse),
      (writeTime * containDisk + readTime * futureUse).toLong,
      (recomp).toLong,
      futureUse,
      numShuffle,
      containDisk == 0)

      // realStages.size * recompTime)
    // logInfo(s"CompDisaggCost $blockId, " +
    //  s"refStages: ${stages.map(f => f.stageId)}, time: $recompTime")
    c.setStageInfo(realStages, recompTime)
    c
  }

}

