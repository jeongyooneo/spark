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
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class RDDLocalMemoryPolicyPoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
   extends DisaggBlockManagerEndpoint(
    rpcEnv, isLocal, conf, listenerBus, blockManagerMaster, thresholdMB) {
  logInfo("RDDCostBasedEvictionEndpoint up")

  val compDiscardRatio = conf.getDouble("spark.disagg.autosizing.comp", defaultValue = 5.0)

  override def taskStartedCall(taskId: String): Unit = {
    rddJobDag match {
      case None =>
        // do nothing
      case Some(dag) =>
        dag.taskStarted(taskId)
    }
  }

  override def stageCompletedCall(stageId: Int): Unit = {
    rddJobDag match {
      case None =>
      // do nothing
      case Some(dag) =>
        dag.removeCompletedStageNode(stageId)
    }
  }

  override def stageSubmittedCall(stageId: Int): Unit = {
    logInfo(s"Stage submitted ${stageId}")
    rddJobDag match {
      case None =>
      // do nothing
      case Some(dag) =>
        dag.stageSubmitted(stageId)
    }
  }

  private val executorBlockMap: mutable.Map[String, mutable.Map[BlockId, Long]] =
    new mutable.HashMap[String, mutable.Map[BlockId, Long]]()

  private val blockCountMap: mutable.Map[BlockId, Int] =
    new mutable.HashMap[BlockId, Int]()

  override def cachingDecision(
                blockId: BlockId, estimateSize: Long,
                taskId: String, executorId: String): Boolean = synchronized {
    val prevTime = prevDiscardTime.get()

    rddJobDag.get.setStoredBlocksCreatedTime(blockId)
    rddJobDag.get.setBlockCreatedTime(blockId)

    // logInfo(s"Request $blockId, size $estimateSize 222")

    val rddId = blockId.asRDDId.get.rddId

    blockCountMap.synchronized {
      if (!blockCountMap.contains(blockId)) {
        blockCountMap.put(blockId, 0)
      }
      blockCountMap.put(blockId, blockCountMap.get(blockId).get + 1)
    }

    executorBlockMap.synchronized {
      // if (totalSize.get() + estimateSize < threshold) {
      logInfo(s"Storing $blockId, " +
        s"size $estimateSize / $totalSize into $executorId")
      rddJobDag.get.storingBlock(blockId)

      if (!executorBlockMap.contains(executorId)) {
        executorBlockMap.put(executorId, new mutable.HashMap[BlockId, Long]())
      }

      val map = executorBlockMap.get(executorId).get
      map.put(blockId, estimateSize)

      return true
    }
  }

  val recentlyEvictFailBlocks: mutable.Map[BlockId, Long] =
    new mutable.HashMap[BlockId, Long]().withDefaultValue(0L)

  override def localEvictionFail(blockId: BlockId, executorId: String, size: Long): Unit = {
    executorBlockMap.synchronized {
      recentlyEvictFailBlocks.put(blockId, System.currentTimeMillis())
     executorBlockMap.get(executorId).get.put(blockId, size)
      blockCountMap.put(blockId, blockCountMap.get(blockId).get + 1)
    }
  }

  override def localEviction(blockId: Option[BlockId],
                             executorId: String, evictionSize: Long): List[BlockId] = {

    val evictionList: mutable.ListBuffer[BlockId] = new ListBuffer[BlockId]

    rddJobDag.get.sortedBlockCost match {
      case None =>
      case Some(l) =>
        executorBlockMap.synchronized {
          val currTime = System.currentTimeMillis()
          executorBlockMap.get(executorId) match {
            case None =>
            case Some(map) =>
              var sizeSum = 0L
              l.foreach {
                pair =>
                  val bid = pair._1
                  val cost = pair._2
                  if (map.contains(bid)) {
                    val elapsed = currTime - recentlyEvictFailBlocks.getOrElse(bid, 0L)
                    if (elapsed > 10000) {

                      recentlyEvictFailBlocks.remove(bid)

                      sizeSum += map.get(bid).get
                      evictionList.append(bid)
                      map.remove(bid)
                      val cnt = blockCountMap.get(bid).get - 1
                      blockCountMap.put(bid, cnt)

                      if (cnt <= 0) {
                        rddJobDag.get.removingBlock(bid)
                      }

                      if (sizeSum > evictionSize) {
                        logInfo(s"LocalDecision] Evict blocks $evictionList " +
                          s"from executor $executorId, size $evictionSize, existing blocks $map")
                        return evictionList.toList
                      }
                    }
                  }
              }
          }
        }
    }


    List.empty
    // throw new RuntimeException(s"Eviction is not performed in $executorId, block:$blockId " +
    //  s".. size: $evictionSize, " +
    //  s"${executorBlockMap.get(executorId)}")
  }

  var prevEvictTime = System.currentTimeMillis()

  override def evictBlocksToIncreaseBenefit(
                totalCompReduction: Long, totalSize: Long): Unit = {
  }

  override def fileWriteEndCall(blockId: BlockId, size: Long): Unit = {
    // rddJobDag.get.storingBlock(blockId, size)
  }

  override def fileRemovedCall(blockInfo: CrailBlockInfo): Unit = {
    rddJobDag.get.removingBlock(blockInfo.bid)
  }

  override def fileCreatedCall(blockInfo: CrailBlockInfo): Unit = {
    // do nothing
  }

  override def fileReadCall(blockInfo: CrailBlockInfo): Unit = {
    // do nothing
  }
}
