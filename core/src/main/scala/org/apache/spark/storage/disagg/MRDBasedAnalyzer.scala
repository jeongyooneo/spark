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

private[spark] class MRDBasedAnalyzer(val rddJobDag: RDDJobDag,
                                      metricTracker: MetricTracker)
  extends CostAnalyzer(metricTracker) with Logging {

  override def compDisaggCost(blockId: BlockId): CompDisaggCost = {
    // val refStages = rddJobDag.getReferenceStages(blockId)
    val mrdStage = rddJobDag.getMRDStage(blockId)


    if (mrdStage == 0) {
      new CompDisaggCost(blockId, 0, 0)
    } else {
      new CompDisaggCost(blockId, 1/Math.max(1.0, mrdStage.toDouble))
    }
  }

}
