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

package org.apache.spark.storage.disaag

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}

import scala.collection.mutable


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class NectarEvictionEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
   extends DisaggBlockManagerEndpoint(
    rpcEnv, isLocal, conf, listenerBus, blockManagerMaster, thresholdMB) {
  logInfo("NectarEvictionEndPoint up")


  val list: mutable.ListBuffer[CrailBlockInfo] = new mutable.ListBuffer[CrailBlockInfo]()

  val rddJobDag = blockManagerMaster.rddJobDag

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

  private def calculateCost(blockInfo: CrailBlockInfo, ct: Long): Long = {
    val a = (blockInfo.size / 10000) * (ct - blockInfo.refTime)
    val b = Math.max(1, blockInfo.refCnt.get()) * rddJobDag.get.blockCompTime(blockInfo.bid, ct)
    a / b
  }

  override def storeBlockOrNot(blockId: BlockId, estimateSize: Long, taskId: String): Boolean = {

    val prevTime = prevDiscardTime.get()


    if (!prevCreatedBlocks.containsKey(blockId)) {
      prevCreatedBlocks.put(blockId, true)
    }

    val rddId = blockId.asRDDId.get.rddId

    synchronized {
      if (disaggBlockInfo.contains(blockId)) {
        return false
      }

      rddJobDag.get.blockCreated(blockId)

      if (totalSize.get() + estimateSize < threshold) {
        logInfo(s"Storing $blockId" +
          s"size $estimateSize / $totalSize, threshold: $threshold")

        blocksSizeToBeCreated.put(blockId, estimateSize)
        totalSize.addAndGet(estimateSize)
        return true
      }

      // discard!!
      var totalCost = 0L
      var totalDiscardSize = 0L

      val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
        new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

      val ct = System.currentTimeMillis()
      if (ct - prevDiscardTime.get() > 1000 &&
        prevDiscardTime.compareAndSet(prevTime, ct)) {
        // cost calculation

        val storedBlocks = disaggBlockInfo.values.toList

        val sortedBlocks = storedBlocks.sortWith((b1, b2) => {
          b1.nectarCost = calculateCost(b1, ct)
          b2.nectarCost = calculateCost(b2, ct)
          b1.nectarCost > b2.nectarCost
        })

        val iterator = sortedBlocks.iterator

        val removalSize = Math.max(estimateSize,
          totalSize.get() + estimateSize - threshold)

        val currTime = System.currentTimeMillis()

        // remove blocks til threshold without considering cost
        // for hard threshold
        while (iterator.hasNext && totalDiscardSize < removalSize) {
          val binfo = iterator.next()
          totalDiscardSize += binfo.size
          removeBlocks.append((binfo.bid, binfo))

          logInfo(s"Try to remove: Cost: ${binfo.nectarCost} " +
                  s"size: $totalDiscardSize/$removalSize, remove block: ${binfo.bid}")
        }
      }

      blockRemoves(removeBlocks)
      removeBlocks.foreach { t =>
        rddJobDag.get.removingBlock(blockId)
      }
    }

    true
  }

  override def fileRemovedCall(blockInfo: CrailBlockInfo): Unit = {
    rddJobDag.get.removingBlock(blockInfo.bid)
  }

  override def fileCreatedCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def fileReadCall(blockInfo: CrailBlockInfo): Unit = {
    // do nothing
    disaggBlockInfo.get(blockInfo.bid) match {
      case None =>
        // do nothing
      case Some(info) =>
        info.refCnt.getAndIncrement()
        info.refTime = System.currentTimeMillis()
    }
  }
}
