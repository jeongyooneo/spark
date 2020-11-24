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
import org.apache.spark.storage.BlockId

private[spark] abstract class EvictionPolicy(sparkConf: SparkConf) {

  val promoteRatio = sparkConf.get(BlazeParameters.PROMOTE_RATIO)

  def decisionLocalEviction(storingCost: CompDisaggCost,
                            executorId: String,
                            blockId: BlockId,
                            estimateSize: Long,
                            onDisk: Boolean): Boolean


  def decisionPromote(storingCost: CompDisaggCost,
                            executorId: String,
                            blockId: BlockId,
                            estimateSize: Long): Boolean

  def selectEvictFromLocal(storingCost: CompDisaggCost,
                           executorId: String,
                           blockId: BlockId,
                           onDisk: Boolean)
                          (func: List[CompDisaggCost] => List[BlockId]): List[BlockId]

  def selectEvictFromDisagg(storingCost: CompDisaggCost,
                            blockId: BlockId)
                           (func: List[CompDisaggCost] => Unit): Unit
}


private[spark] object EvictionPolicy {
  def apply(costAnalyzer: CostAnalyzer,
            metricTracker: MetricTracker,
            sparkConf: SparkConf): EvictionPolicy = {

    val policy = sparkConf.get(BlazeParameters.EVICTION_POLICY)

    if (policy.equals("Default")) {
      new DefaultEvictionPolicy(costAnalyzer, metricTracker, sparkConf)
    } else if (policy.equals("Cost-based")) {
      new OnlyCostBasedEvictionPolicy(costAnalyzer, metricTracker, sparkConf)
    } else if (policy.equals("RDD-Ordering")) {
      // new RddOrderingEvictionPolicy(costAnalyzer, metricTracker, sparkConf)
      throw new RuntimeException(s"Unsupported evictionPolicy $policy")
    }
    else if (policy.equals("Cost-size-ratio")) {
      new CostSizeRatioBasedEvictionPolicy(costAnalyzer, metricTracker, sparkConf)
    } else if (policy.equals("Cost-size-ratio2")) {
      new CostSizeRatioBased2EvictionPolicy(costAnalyzer, metricTracker, sparkConf)
    }
    else {
      throw new RuntimeException(s"Unsupported evictionPolicy $policy")
    }
  }
}




