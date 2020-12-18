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

private[spark] class LCSCostAnalyzer(val rddJobDag: RDDJobDag,
                                     metricTracker: MetricTracker)
  extends CostAnalyzer(metricTracker) with Logging {

  val writeThp = BlazeParameters.writeThp
  private val readThp = BlazeParameters.readThp

  override def compDisaggCost(executorId: String, blockId: BlockId): CompDisaggCost = {
    // val refCnt = rddJobDag.getLRCRefCnt(blockId)

    val node = rddJobDag.getRDDNode(blockId)
    val refCnt = rddJobDag.getReferenceStages(blockId)
      .filter(p => node.getStages.contains(p.stageId)).size

    // we do not consider disagg overhead here
    val (recompTime, numShuffle) = rddJobDag.blockCompTime(blockId,
      metricTracker.blockCreatedTimeMap.get(blockId))

    val writeTime = (metricTracker.getBlockSize(blockId) * writeThp).toLong
    var readTime = (metricTracker.getBlockSize(blockId) * readThp).toLong

    new CompDisaggCost(blockId,
      Math.min(recompTime * refCnt, writeTime + readTime * refCnt),
      writeTime + readTime * refCnt,
      recompTime * refCnt,
      refCnt,
      numShuffle,
      recompTime,
      writeTime,
      readTime,
      false)
  }
}
