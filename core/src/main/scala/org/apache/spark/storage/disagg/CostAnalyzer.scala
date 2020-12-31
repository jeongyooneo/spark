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
import scala.collection.mutable.ListBuffer

private[spark] abstract class CostAnalyzer(val metricTracker: MetricTracker) extends Logging {

  @volatile
  var sortedBlockByCompCostInLocal: AtomicReference[Map[String,
    mutable.ListBuffer[CompDisaggCost]]] =
    new AtomicReference[Map[String, mutable.ListBuffer[CompDisaggCost]]](null)

  @volatile
  var sortedBlockByCompCostInDiskLocal: AtomicReference[Map[String,
    mutable.ListBuffer[CompDisaggCost]]] =
    new AtomicReference[Map[String, mutable.ListBuffer[CompDisaggCost]]](null)

  @volatile
  var sortedBlockByCompCostInDisagg: Option[List[CompDisaggCost]] = None

  // For cost analysis
  def compDisaggCost(executorId: String, blockId: BlockId): CompDisaggCost
  def compDisaggCostWithTaskAttemp(executorId: String, blockId: BlockId,
                                   taskAttemp: Long): CompDisaggCost
  = compDisaggCost(executorId, blockId)

  def update: Unit = {

    var totalSize: Long = 0L

    val updateStart = System.currentTimeMillis()

    val localLMap = metricTracker.getExecutorLocalMemoryBlocksMap
      .map(entry => {
        val executorId = entry._1
        val blocks = entry._2
        val l = blocks.map(blockId => {
          compDisaggCost(executorId, blockId)
        }).toList
        (executorId, l)
      })

    val localDiskMap = metricTracker.getExecutorLocalDiskBlocksMap
      .map(entry => {
        val executorId = entry._1
        val blocks = entry._2
        val l = blocks.map(blockId => {
          compDisaggCost(executorId, blockId)
        }).toList
        (executorId, l)
      })


    var elapsed = System.currentTimeMillis() - updateStart

    totalSize = Math.max(1, totalSize)

    var start = System.currentTimeMillis()

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompCostInDisagg took $elapsed ms")


    start = System.currentTimeMillis()
    sortedBlockByCompCostInLocal.set(
      localLMap.map(entry => {
        val l = entry._2
        (entry._1, l.sortWith(_.cost < _.cost).to[ListBuffer])
      }))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompCostInLocal took $elapsed ms")

    start = System.currentTimeMillis()
    sortedBlockByCompCostInDiskLocal.set(
      localDiskMap.map(entry => {
        val l = entry._2
        (entry._1, l.sortWith(_.cost < _.cost).to[ListBuffer])
      }))

    elapsed = System.currentTimeMillis() - start
    // logInfo(s"costAnalyzer.update: sortedBlockByCompCostInDiskLocal took $elapsed ms")

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
            if (cost.futureUse <= 0) {
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
              if (cost.futureUse <= 0) {
                if (!zeros.contains(cost.blockId.asRDDId.get.rddId)) {
                  zeros.add(cost.blockId.asRDDId.get.rddId)
                  logInfo(s"Zero RDD in LocalMem: ${cost.blockId.asRDDId.get.rddId}")
                }
              } else {
                if (!nonzeros.contains(cost.blockId.asRDDId.get.rddId)) {
                  nonzeros.add(cost.blockId.asRDDId.get.rddId)
                  logInfo(s"Nonzero RDD in LocalMem: ${cost.blockId.asRDDId.get.rddId}, " +
                    s"${cost.blockId}, futureuse: ${cost.futureUse}")
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
              if (cost.futureUse <= 0) {
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
                     var cost: Double,
                     var disaggCost: Long = 0,
                     var compCost: Long = 0,
                     var futureUse: Double = 0,
                     val numShuffle: Int = 0,
                     var recompTime: Long = 0L,
                     var writeTime: Long = 0L,
                     val readTime: Long = 0L,
                     val onDisk: Boolean = false,
                     var updated: Boolean = false) extends Logging {

  var stages: Option[List[StageDistance]] = None
  var updatedBlocks: Option[mutable.HashSet[BlockId]] = None

  def addRecomp(t: Long): Unit = {
    logInfo(s"Updating ${blockId} recomp with ${t}")
    recompTime += t
    compCost = (recompTime * futureUse).toLong
    cost = Math.min(compCost, disaggCost)
    updated = true
  }

  def increaseFutureUse(u: Int): Unit = {
    logInfo(s"Updating ${blockId} future use with ${u}")
    futureUse += u
    compCost = (recompTime * futureUse).toLong
    disaggCost = (writeTime + readTime * futureUse).toLong
    cost = Math.min(compCost, disaggCost)
    updated = true
  }

  def setStageInfo(s: List[StageDistance], time: Long): Unit = {
    stages = Some(s)
  }
}


object CostAnalyzer {
  def apply(sparkConf: SparkConf,
            rDDJobDag: Option[RDDJobDag],
            metricTracker: MetricTracker): CostAnalyzer = {
    val costType = sparkConf.get(BlazeParameters.COST_FUNCTION)

      if (costType.equals("Blaze-Disk-Recomp")) {
        new BlazeRecompAndDiskCostAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-Disk-Only")) {
        new BlazeDiskCostAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-Disk-Future-Use")) {
        new BlazeDiskCostFutureUseAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Blaze-Recomp-Only")) {
        new BlazeRecompCostOnlyAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("MRD")) {
        new MRDBasedAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("No")) {
        new NoCostAnalyzer(metricTracker)
      } else if (costType.equals("Spark-Autocaching")) {
        new SparkAutocachingAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("Spark-Autocaching-CC")) {
        new SparkAutocachingAnalyzerCC(rDDJobDag.get, metricTracker)
      } else if (costType.equals("LRC")) {
        new LRCCostAnalyzer(rDDJobDag.get, metricTracker)
      } else if (costType.equals("LCS")) {
        new LCSCostAnalyzer(rDDJobDag.get, metricTracker)
      } else {
        throw new RuntimeException(s"Unsupported cost function: $costType")
      }
  }
}

