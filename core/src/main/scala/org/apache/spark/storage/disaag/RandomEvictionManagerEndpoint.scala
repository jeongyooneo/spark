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
class RandomEvictionManagerEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
  extends DisaggBlockManagerEndpoint(
    rpcEnv, isLocal, conf, listenerBus, blockManagerMaster, thresholdMB) {

  logInfo("RandomEvictionManagerEndpoint up")


  override def fileCreatedCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def fileReadCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def fileRemovedCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def taskStartedCall(taskId: String): Unit = {

  }

  override def stageCompletedCall(stageId: Int): Unit = {

  }

  override def stageSubmittedCall(stageId: Int): Unit = {

  }

  override def storeBlockOrNot(blockId: BlockId, estimateSize: Long, taskId: String): Boolean = {

    val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
      new mutable.ListBuffer[(BlockId, CrailBlockInfo)]
    val prevTime = prevDiscardTime.get()

    val elapsed = System.currentTimeMillis() - prevTime

    if (totalSize.get() + estimateSize > threshold && elapsed > 1000) {
      // discard!!
      // rm 1/3 after 10 seconds
      if (prevDiscardTime.compareAndSet(prevTime, System.currentTimeMillis())) {

        // logInfo(s"Discard blocks.. pointer ${lruPointer} / ${lruQueue.size}")
        // val targetDiscardSize: Long = 1 * (disaggTotalSize + estimateSize) / 3

        logInfo(s"lruQueue: $lruQueue")

        val targetDiscardSize: Long = Math.max(totalSize.get()
          + estimateSize - threshold,
          2L * 1000L * 1000L * 1000L) // 5GB

        var totalDiscardSize: Long = 0

        lruQueue.synchronized {
          var cnt = 0

          val lruSize = lruQueue.size

          val currTime = System.currentTimeMillis();

          while (totalDiscardSize < targetDiscardSize && lruQueue.nonEmpty && cnt < lruSize) {
            val candidateBlock: CrailBlockInfo = lruQueue.head

            if (timeToRemove(candidateBlock.createdTime, currTime)) {
              totalDiscardSize += candidateBlock.size
              logInfo(s"Discarding ${candidateBlock.bid}.." +
                s"pointer ${lruPointer} / ${lruQueue.size}" +
                s"size $totalDiscardSize / $targetDiscardSize")
              removeBlocks.append((candidateBlock.bid, candidateBlock))

              lruQueue -= candidateBlock

              cnt += 1
            }
          }
        }
      }
    }

    blocksSizeToBeCreated.put(blockId, estimateSize)
    totalSize.addAndGet(estimateSize)

    blockRemoves(removeBlocks)
    true
  }
}
