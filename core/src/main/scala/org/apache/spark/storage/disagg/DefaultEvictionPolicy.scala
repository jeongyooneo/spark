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
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

private[spark] class DefaultEvictionPolicy(
           val costAnalyzer: CostAnalyzer,
           val metricTracker: MetricTracker,
           sparkConf: SparkConf) extends EvictionPolicy(sparkConf) with Logging {

  def decisionLocalEviction(storingCost: CompDisaggCost,
                            executorId: String,
                            blockId: BlockId,
                            estimateSize: Long): Boolean = {
    // wheter to store it into local or disagg
    // first, check cost in local executors
    val lowCompDisaggBlocks =
    costAnalyzer.findLowCompDisaggCostBlocks(storingCost, executorId, blockId, false)

    if (lowCompDisaggBlocks.isEmpty) {
      // the rdd to be stored has minimum reduction, so we discard it
      false
    } else {

      val total = lowCompDisaggBlocks
        .map(x => metricTracker.getBlockSize(x.blockId))
        .sum

      if (total > estimateSize) {
        // we store this rdd and evict others
        true
      } else {
        // discard the rdd because
        // the sum of block sizes is smaller than the size of rdd to be stored
        false
      }
    }
  }


  def decisionPromote(storingCost: CompDisaggCost,
                            executorId: String,
                            blockId: BlockId,
                            estimateSize: Long): Boolean = {
    decisionLocalEviction(storingCost, executorId, blockId, estimateSize)
  }

  def selectEvictFromLocal(storingCost: CompDisaggCost,
                           executorId: String,
                           blockId: BlockId)
                          (func: List[CompDisaggCost] => List[BlockId]): List[BlockId] = {
    val lowCompDisaggBlocks = costAnalyzer
      .findLowCompDisaggCostBlocks(storingCost, executorId, blockId, true)
    func(lowCompDisaggBlocks)
  }

  def selectEvictFromDisagg(storingCost: CompDisaggCost,
                            blockId: BlockId)
                           (func: List[CompDisaggCost] => Unit): Unit = {
    val list = costAnalyzer.findLowCompHighDisaggCostBlocksInDisagg(storingCost, blockId)
    func(list)
  }
}




