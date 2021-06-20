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

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

import scala.collection.mutable

private[spark] abstract class EvictionPolicy(sparkConf: SparkConf) {

  val promotionRatio = sparkConf.get(BlazeParameters.PROMOTION_RATIO)

  def promote(storingCost: CompCost,
              executorId: String,
              blockId: BlockId,
              estimateSize: Long): Boolean

  def selectLocalEvictionCandidate(storingCost: CompCost,
                                   executorId: String,
                                   evictSize: Long,
                                   onDisk: Boolean)
                                  (func: mutable.ListBuffer[CompCost]
                                    => List[BlockId]): List[BlockId]
}


private[spark] object EvictionPolicy {
  def apply(costAnalyzer: CostAnalyzer,
            metricTracker: MetricTracker,
            sparkConf: SparkConf): EvictionPolicy = {

    val policy = sparkConf.get(BlazeParameters.EVICTION_POLICY)

    if (policy.equals("Default") || policy.equals("Cost-based")) {
      new OnlyCostBasedEvictionPolicy(costAnalyzer, metricTracker, sparkConf)
    } else if (policy.equals("Cost-size-ratio2")) {
      new CostSizeRatioBased2EvictionPolicy(costAnalyzer, metricTracker, sparkConf)
    }
    else {
      throw new RuntimeException(s"Unsupported evictionPolicy $policy")
    }
  }
}




