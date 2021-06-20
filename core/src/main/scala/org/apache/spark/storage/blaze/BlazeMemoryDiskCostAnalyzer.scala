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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable

private[spark] class BlazeMemoryDiskCostAnalyzer(val rddJobDag: RDDJobDag,
                                                 metricTracker: MetricTracker)
  extends CostAnalyzer(metricTracker) with Logging {

  // 10Gib per sec to byte per sec
  private val BANDWIDTH = (10 / 8.0) * 1024 * 1024 * 1024.toDouble
  val writeThp = BlazeParameters.writeThp
  private val readThp = BlazeParameters.readThp

  override def compCostWithTaskAttempt(executorId: String,
                                       blockId: BlockId,
                                       taskAttemp: Long): CompCost = {
    val node = rddJobDag.getRDDNode(blockId)
    val stages = rddJobDag.getReferenceStages(blockId)
    val (recompTime, numShuffle) = rddJobDag.blockCompTime(blockId,
      metricTracker.blockCreatedTimeMap.get(blockId))

    val realStages = stages // .filter(p => node.getStages.contains(p.stageId))
      // .filter(p => p.stageId != node.rootStage)

    // logInfo(s"Compute RDD ${node.rddId} stages ${node.getStages}")

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
    var futureUse = realStages.size + currentUsage
    val writeTime = (metricTracker.getBlockSize(blockId) * writeThp).toLong
    var readTime = (metricTracker.getBlockSize(blockId) * readThp).toLong

    futureUse = utilCost(futureUse, rddJobDag, node)

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

    val c = new CompCost(blockId,
      Math.min(recomp, writeTime * containDisk + readTime * futureUse),
      (writeTime * containDisk + readTime * futureUse).toLong,
      (recomp).toLong,
      futureUse,
      numShuffle,
      recompTime,
      writeTime * containDisk,
      readTime,
      containDisk == 0)

    c.setStageInfo(realStages, recompTime)
    c
  }

  override def compCost(executorId: String,
                        blockId: BlockId): CompCost = {
    val node = rddJobDag.getRDDNode(blockId)
    val stages = rddJobDag.getReferenceStages(blockId)
    val (recompTime, numShuffle) = rddJobDag.blockCompTime(blockId,
     metricTracker.blockCreatedTimeMap.get(blockId))

    // val realStages = stages.filter(p => node.getStages.contains(p.stageId))
    val realStages = stages // .filter(p => node.getStages.contains(p.stageId))

    // logInfo(s"Compute RDD ${node.rddId} stages ${node.getStages}")

     val containDisk = if (metricTracker
      .localDiskStoredBlocksMap.containsKey(executorId)
       && metricTracker.localDiskStoredBlocksMap.get(executorId).contains(blockId)) {
       0
     } else {
       1
     }

    // val futureUse = realStages.size.map(x => Math.pow(0.5, x.prevCached)).sum
    var futureUse = realStages.size
    val writeTime = (metricTracker.getBlockSize(blockId) * writeThp).toLong
    var readTime = (metricTracker.getBlockSize(blockId) * readThp).toLong


    // Check repeated pattern if the usage is zero
    if (futureUse == 0 && rddJobDag.profiledJob <= metricTracker.currJob.get()) {
      val repeatedNode = rddJobDag
        .findRepeatedNode(node, node, new mutable.HashSet[RDDNode]())
      repeatedNode match {
        case Some(rnode) =>
          val crossJobRef = rddJobDag.numCrossJobReference(rnode)
          if (node.jobId == metricTracker.currJob.get() && crossJobRef > 0) {
            futureUse = crossJobRef
            logDebug(s"Added crossJobRef for rdd ${node.rddId}, job ${node.jobId}, " +
              s"currJob ${metricTracker.currJob}" +
              s"add $crossJobRef")
          }
        case None =>
          // If this rdd is reference consequently in the previous jobs
          var result =
            rddJobDag.getReferencedJobs(node)
              .contains(metricTracker.currJob.get()) &&
              rddJobDag.getReferencedJobs(node)
                .contains(metricTracker.currJob.get() - 1)

          if (!result) {
            // This means that this node will be referenced in the future
            result = node.crossReferenced && node.jobId + 1 > metricTracker.currJob.get()
          }

          logDebug(s"No repeatedNode for ${node.rddId}, " +
            s"check conseuctive job reference, " +
            s"currjob ${metricTracker.currJob.get()}, " +
            s"refJob ${rddJobDag.getReferencedJobs(node)}, " +
            s"consecutive: ${result}, " +
            s"crossReference: ${node.crossReferenced} " +
            s"jobId: ${node.jobId}")

          if (result) {
            futureUse += 2
          }
      }
    }

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

    val c = new CompCost(blockId,
      Math.min(recomp, writeTime * containDisk + readTime * futureUse),
      writeTime * containDisk + readTime * futureUse,
      recomp,
      futureUse,
      numShuffle,
      recompTime,
      writeTime,
      readTime,
      containDisk == 0)

    c.setStageInfo(realStages, recompTime)
    c
  }

}

