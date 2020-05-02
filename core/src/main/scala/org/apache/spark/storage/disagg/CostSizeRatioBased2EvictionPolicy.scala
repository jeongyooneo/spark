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

private[spark] class CostSizeRatioBased2EvictionPolicy(
           val costAnalyzer: CostAnalyzer,
           val metricTracker: MetricTracker,
           conf: SparkConf) extends EvictionPolicy(conf) with Logging {

  def decisionLocalEviction(storingCost: CompDisaggCost,
                            executorId: String,
                            blockId: BlockId,
                            estimateSize: Long): Boolean = {
    if (costAnalyzer.sortedBlockByCompCostInLocal.get() != null) {
      val l = costAnalyzer.sortedBlockByCompCostInLocal.get()(executorId)
      if (l.isEmpty) {
        false
      } else {
        if (l.head.reduction > storingCost.reduction) {
          false
        } else {
          true
        }
      }
    } else {
      false
    }
  }

  def decisionPromote(storingCost: CompDisaggCost,
                      executorId: String,
                      blockId: BlockId,
                      estimateSize: Long): Boolean = {
    if (costAnalyzer.sortedBlockByCompCostInLocal.get() != null) {
      val l = costAnalyzer.sortedBlockByCompCostInLocal.get()(executorId)
      if (l.isEmpty) {
        false
      } else {
        val index = ((l.size - 1) * promoteRatio).toInt
        if (l(index).reduction > storingCost.reduction) {
          false
        } else {
          true
        }
      }
    } else {
      false
    }
  }

  def selectEvictFromLocal(storingCost: CompDisaggCost,
                           executorId: String,
                           blockId: BlockId)
                          (func: List[CompDisaggCost] => List[BlockId]): List[BlockId] = {
    if (costAnalyzer.sortedBlockByCompCostInLocal.get() != null) {
      val l =
        costAnalyzer.sortedBlockByCompCostInLocal.get()(executorId)
          .filter(p => {
            if (p.reduction <= 0) {
              true
            } else {
              val costRatio = storingCost.reduction / p.reduction
              val sizeRatio =
                metricTracker.getBlockSize(blockId) / metricTracker.getBlockSize(p.blockId)
              costRatio > sizeRatio
            }
          })
      func(l)
    } else {
      func(List.empty)
    }
  }

  def selectEvictFromDisagg(storingCost: CompDisaggCost,
                            blockId: BlockId)
                           (func: List[CompDisaggCost] => Unit): Unit = {
    costAnalyzer.sortedBlockByCompCostInDisagg match {
      case None =>
      case Some(l) =>
        val zero = l.filter(p => {
            if (p.reduction <= 0) {
              true
            } else {
              val costRatio = storingCost.reduction / p.reduction
              val sizeRatio =
                metricTracker.getBlockSize(blockId) / metricTracker.getBlockSize(p.blockId)
              costRatio > sizeRatio
            }
          })
        func(zero)
    }
  }
}




