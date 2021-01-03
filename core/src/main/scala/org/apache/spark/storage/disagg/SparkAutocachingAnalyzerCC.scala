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

import scala.collection.mutable

private[spark] class SparkAutocachingAnalyzerCC(val rddJobDag: RDDJobDag,
                                                metricTracker: MetricTracker)
  extends CostAnalyzer(metricTracker) with Logging {

  // 10Gib per sec to byte per sec
  private val BANDWIDTH = (10 / 8.0) * 1024 * 1024 * 1024.toDouble

  override def compDisaggCost(executorId: String, blockId: BlockId): CompDisaggCost = {
    val node = rddJobDag.getRDDNode(blockId)

    // val futureUse = realStages.size.map(x => Math.pow(0.5, x.prevCached)).sum
    var futureUse = rddJobDag.getReferenceStages(blockId).size

    // Check repeated pattern if the usage is zero
    if (futureUse == 0) {
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

    val c = new CompDisaggCost(blockId,
      futureUse,
      0,
      futureUse,
      futureUse,
      0)

      // realStages.size * recompTime)
    // logInfo(s"CompDisaggCost $blockId, " +
    //  s"refStages: ${stages.map(f => f.stageId)}, time: $recompTime")
    c
  }

}

