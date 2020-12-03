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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.disagg.RDDJobDag.StageDistance

import scala.collection.mutable

private[spark] abstract class CostAnalyzer(val metricTracker: MetricTracker) extends Logging {

  @volatile
  var sortedBlockByCompCostInLocal: AtomicReference[Map[String, List[CompDisaggCost]]] =
    new AtomicReference[Map[String, List[CompDisaggCost]]](null)

  @volatile
  var sortedBlockByCompCostInDiskLocal: AtomicReference[Map[String, List[CompDisaggCost]]] =
    new AtomicReference[Map[String, List[CompDisaggCost]]](null)

  @volatile
  var sortedBlockByCompSizeRatioInLocal: AtomicReference[Map[String, List[CompDisaggCost]]] =
    new AtomicReference[Map[String, List[CompDisaggCost]]](null)

  @volatile
  var sortedBlockByCompCostInDisagg: Option[List[CompDisaggCost]] = None

  @volatile
  var sortedBlockByCompSizeRatioInDisagg: Option[List[CompDisaggCost]] = None

  // For cost analysis
  def compDisaggCost(blockId: BlockId): CompDisaggCost

  // this is for local decision
  def findLowCompDisaggCostBlocks(compDisaggCost: CompDisaggCost,
                                  executorId: String,
                                  blockId: BlockId,
                                  retrieveAll: Boolean): List[CompDisaggCost] = {
    val lowDisaggLowCost = new mutable.ListBuffer[CompDisaggCost]
    val highDisaggLowCost = new mutable.ListBuffer[CompDisaggCost]
    val lowDisaggHighCost = new mutable.ListBuffer[CompDisaggCost]
    val higDisaggHighCost = new mutable.ListBuffer[CompDisaggCost]

    if (sortedBlockByCompCostInLocal.get() != null) {
      val map = sortedBlockByCompCostInLocal.get()
      map(executorId).foreach {
        elem =>
          if (elem.disaggCost > compDisaggCost.disaggCost
            && elem.reduction < compDisaggCost.reduction) {
            if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
              highDisaggLowCost.append(elem)
            }
          } else if (elem.disaggCost <= compDisaggCost.disaggCost
            && elem.reduction >= compDisaggCost.reduction) {
            if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
              lowDisaggHighCost.append(elem)
            }
          } else if (elem.disaggCost > compDisaggCost.disaggCost
            && elem.reduction >= compDisaggCost.reduction) {
            if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
              higDisaggHighCost.append(elem)
            }
          } else if (elem.disaggCost <= compDisaggCost.disaggCost
            && elem.reduction < compDisaggCost.reduction) {
            if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
              lowDisaggLowCost.append(elem)
            }
          }
      }
    }

    if (retrieveAll) {
      lowDisaggLowCost.appendAll(highDisaggLowCost)
    }

    lowDisaggLowCost.toList
  }

  // This is for disagg decision
  def findLowCompHighDisaggCostBlocksInDisagg(compDisaggCost: CompDisaggCost,
                                              blockId: BlockId):
    List[CompDisaggCost] = {
    val highDisaggLowCost = new mutable.ListBuffer[CompDisaggCost]
    val lowDisaggLowCost = new mutable.ListBuffer[CompDisaggCost]
    val higDisaggHighCost = new mutable.ListBuffer[CompDisaggCost]
    val lowDisaggHighCost = new mutable.ListBuffer[CompDisaggCost]

    sortedBlockByCompCostInDisagg match {
      case None =>
      case Some(l) =>
        l.foreach {
          elem =>
            if (elem.disaggCost > compDisaggCost.disaggCost
              && elem.reduction < compDisaggCost.reduction) {
              if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
                highDisaggLowCost.append(elem)
              }
            } else if (elem.disaggCost <= compDisaggCost.disaggCost
            && elem.reduction >= compDisaggCost.reduction) {
              if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
                lowDisaggHighCost.append(elem)
              }
            } else if (elem.disaggCost > compDisaggCost.disaggCost
            && elem.reduction >= compDisaggCost.reduction) {
              if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
                higDisaggHighCost.append(elem)
              }
            } else if (elem.disaggCost <= compDisaggCost.disaggCost
            && elem.reduction < compDisaggCost.reduction) {
              if (blockId.asRDDId.get.rddId != elem.blockId.asRDDId.get.rddId) {
                lowDisaggLowCost.append(elem)
              }
            }
        }
    }

    highDisaggLowCost.appendAll(lowDisaggHighCost)
    highDisaggLowCost.appendAll(higDisaggHighCost)
    highDisaggLowCost.appendAll(lowDisaggHighCost)
    highDisaggLowCost.toList
  }

  def update: Unit = {

    var totalSize: Long = 0L

    val disaggL = metricTracker
      .getDisaggBlocks.map(blockId => {
      compDisaggCost(blockId)
    }).toList

    val updateStart = System.currentTimeMillis()

    val localLMap = metricTracker.getExecutorLocalMemoryBlocksMap
      .map(entry => {
        val executorId = entry._1
        val blocks = entry._2
        val l = blocks.map(blockId => {
          compDisaggCost(blockId)
        }).toList
        (executorId, l)
      })

    val localDiskMap = metricTracker.getExecutorLocalDiskBlocksMap
      .map(entry => {
        val executorId = entry._1
        val blocks = entry._2
        val l = blocks.map(blockId => {
          compDisaggCost(blockId)
        }).toList
        (executorId, l)
      })


    var elapsed = System.currentTimeMillis() - updateStart

    totalSize = Math.max(1, totalSize)

    var start = System.currentTimeMillis()

    sortedBlockByCompCostInDisagg =
      Some(disaggL.sortWith(_.reduction < _.reduction))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompCostInDisagg took $elapsed ms")

    start = System.currentTimeMillis()
    sortedBlockByCompSizeRatioInDisagg =
      Some(disaggL.sortWith((x, y) => {
        val b1 = x.reduction / Math.max(1, metricTracker.getBlockSize(x.blockId).toDouble)
        val b2 = y.reduction / Math.max(1, metricTracker.getBlockSize(y.blockId).toDouble)
        b1 < b2
      }))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompSizeRatioInDisagg took $elapsed ms")

    start = System.currentTimeMillis()
    sortedBlockByCompCostInLocal.set(
      localLMap.map(entry => {
        val l = entry._2
        (entry._1, l.sortWith(_.reduction < _.reduction))
      }))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompCostInLocal took $elapsed ms")

    start = System.currentTimeMillis()
    sortedBlockByCompCostInDiskLocal.set(
      localDiskMap.map(entry => {
        val l = entry._2
        (entry._1, l.sortWith(_.reduction < _.reduction))
      }))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompCostInDiskLocal took $elapsed ms")

    start = System.currentTimeMillis()
    sortedBlockByCompSizeRatioInLocal.set(
      localLMap.map(entry => {
        val l = entry._2
        (entry._1, l.sortWith((x, y) => {
          val b1 = x.reduction / Math.max(1, metricTracker.getBlockSize(x.blockId).toDouble)
          val b2 = y.reduction / Math.max(1, metricTracker.getBlockSize(y.blockId).toDouble)
          b1 < b2
        }))
      }))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompSizeRatioInLocal took $elapsed ms")

    /*
    val sb = new StringBuilder
    sortedBlockByCompCostInLocal.get()
      .map(entry => s"${entry._1} -> ${entry._2}")
      .foreach {
        s => sb.append(s)
          sb.append("\n")
      }

    logInfo(s"------------- Cost map -----------\n${sb.toString()}\n----------------\n")
    */

  }

  def findZeroCostRDDs: collection.Set[Int] = {
    update

    val zeros: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    val nonzeros: mutable.HashSet[Int] = new mutable.HashSet[Int]()

    logInfo(s"Find zero cost RDD")

    sortedBlockByCompCostInDisagg match {
      case None =>
      case Some(l) =>
        logInfo(s"Find zero disagg len: ${l.size}")
        l.foreach {
          cost =>
            if (cost.reduction <= 0) {
              if (!zeros.contains(cost.blockId.asRDDId.get.rddId)) {
                zeros.add(cost.blockId.asRDDId.get.rddId)
                logInfo(s"Zero RDD in Disagg: ${cost.blockId.asRDDId.get.rddId}")
              }
            } else {
              if (!nonzeros.contains(cost.blockId.asRDDId.get.rddId)) {
                nonzeros.add(cost.blockId.asRDDId.get.rddId)
                logInfo(s"Nonzero RDD in Disagg: ${cost.blockId.asRDDId.get.rddId}")
              }
            }
        }
    }

    if (sortedBlockByCompCostInLocal.get() != null) {
      val map = sortedBlockByCompCostInLocal.get()
      map.values.foreach {
        l =>
          logInfo(s"Find zero local len: ${l.size}")
          l.foreach {
            cost =>
              if (cost.reduction <= 0) {
                if (!zeros.contains(cost.blockId.asRDDId.get.rddId)) {
                  zeros.add(cost.blockId.asRDDId.get.rddId)
                  logInfo(s"Zero RDD in LocalMem: ${cost.blockId.asRDDId.get.rddId}")
                }
              } else {
                if (!nonzeros.contains(cost.blockId.asRDDId.get.rddId)) {
                  nonzeros.add(cost.blockId.asRDDId.get.rddId)
                  logInfo(s"Nonzero RDD in LocalMem: ${cost.blockId.asRDDId.get.rddId}")
                }
              }
          }
      }
    }

    if (sortedBlockByCompCostInDiskLocal.get() != null) {
      val map = sortedBlockByCompCostInDiskLocal.get()
      map.values.foreach {
        l =>
          logInfo(s"Find zero disk len: ${l.size}")
          l.foreach {
            cost =>
              if (cost.reduction <= 0) {
                if (!zeros.contains(cost.blockId.asRDDId.get.rddId)) {
                  zeros.add(cost.blockId.asRDDId.get.rddId)
                  logInfo(s"Zero RDD in Disk: ${cost.blockId.asRDDId.get.rddId}")
                }
              } else {
                if (!nonzeros.contains(cost.blockId.asRDDId.get.rddId)) {
                  nonzeros.add(cost.blockId.asRDDId.get.rddId)
                  logInfo(s"Nonzero RDD in Disk: ${cost.blockId.asRDDId.get.rddId}")
                }
              }
          }
      }
    }

    zeros.diff(nonzeros)
  }

  class DisaggOverhead(val blockId: BlockId,
                       val cost: Long)

  class CompReduction(val blockId: BlockId,
                  val reduction: Long) {
  }

}

class CompDisaggCost(val blockId: BlockId,
                     val disaggCost: Long,
                     val reduction: Double) {
  override def toString: String = {
    s"($blockId,$reduction,$disaggCost)"
  }

  var stages: Option[List[StageDistance]] = None
  var compTime = 0L

  def setStageInfo(s: List[StageDistance], time: Long): Unit = {
    stages = Some(s)
    compTime = time
  }
}


object CostAnalyzer {
  def apply(sparkConf: SparkConf,
            rDDJobDag: Option[RDDJobDag],
            metricTracker: MetricTracker): CostAnalyzer = {
    val costType = sparkConf.get(BlazeParameters.COST_FUNCTION)

      if (costType.equals("Blaze")) {
        new BlazeCostAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-No-Disagg")) {
        new BlazeCostNoDisaggAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-MRD")) {
        new BlazeCostMRDAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-Time-Only")) {
        new BlazeCostOnlyRecompTimeAnalyzer(rDDJobDag.get, metricTracker)
      }
      else if (costType.equals("Blaze-Stage-Ref")) {
        new BlazeCostStageRefCntAnalyzer(rDDJobDag.get, metricTracker)
      }
      else if (costType.equals("Blaze-Ref-Only")) {
        new BlazeCostOnlyRefCntAnalyzer(rDDJobDag.get, metricTracker)
      }
      else if (costType.equals("Blaze-Leaf-Cnt")) {
        new BlazeCostLeafCntAnalyzer(rDDJobDag.get, metricTracker)
      }
      else if (costType.equals("Blaze-Linear-Dist")) {
        new BlazeCostLinearDistAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-Ref-Cnt")) {
        new BlazeCostRefCntAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("MRD")) {
        new MRDBasedAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("No")) {
        new NoCostAnalyzer(metricTracker)
      } else if (costType.equals("LRC")) {
        new LRCCostAnalyzer(rDDJobDag.get, metricTracker)
      } else {
        throw new RuntimeException(s"Unsupported cost function: $costType")
      }
  }
}

