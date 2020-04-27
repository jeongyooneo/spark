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

private[spark] class BlazeCostMRDAnalyzer(val rddJobDag: RDDJobDag,
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

  override def compDisaggCost(blockId: BlockId): CompDisaggCost = {
    val refStages = rddJobDag.getReferenceStages(blockId)
    val recompTime = rddJobDag.getRecompTime(blockId,
     metricTracker.blockCreatedTimeMap.get(blockId))

    if (refStages.isEmpty) {
      new CompDisaggCost(blockId, 0, 0)
    } else {
      new CompDisaggCost(blockId,
        disaggCostCalc(blockId, metricTracker.getBlockSize(blockId), 1),
        refStages.map(x => 1.0 / x.distance).sum * recompTime)
    }

  }

}
