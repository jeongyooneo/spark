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

  val writeThp = 15000.0 / (600 * 1024 * 1024)
  val readThp = 10000.0 / (600 * 1024 * 1024)

  override def compDisaggCost(blockId: BlockId): CompDisaggCost = {
    val node = rddJobDag.getRDDNode(blockId)
    val stages = rddJobDag.getReferenceStages(blockId)
    val recompTime = rddJobDag.blockCompTime(blockId,
     metricTracker.blockCreatedTimeMap.get(blockId))

    val realStages = stages.filter(p => node.getStages.contains(p.stageId))

    // val futureUse = realStages.size.map(x => Math.pow(0.5, x.prevCached)).sum
    val futureUse = realStages.size
    val writeTime = (metricTracker.getBlockSize(blockId) * writeThp).toLong
    val readTime = (metricTracker.getBlockSize(blockId) * readThp).toLong

    val c = new CompDisaggCost(blockId,
      writeTime + readTime * futureUse,
      (writeTime + readTime * futureUse).toLong,
      Long.MaxValue,
      futureUse)

      // realStages.size * recompTime)
    // logInfo(s"CompDisaggCost $blockId, " +
    //  s"refStages: ${stages.map(f => f.stageId)}, time: $recompTime")
    c.setStageInfo(realStages, recompTime)
    c
  }

}

