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

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicLong

import org.apache.crail.{CrailLocationClass, CrailNodeType, CrailStorageClass}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.disaag.DisaggBlockManagerMessages._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}
import org.apache.spark.util.ThreadUtils

import scala.collection.convert.decorateAsScala._
import scala.collection.{mutable, _}
import scala.concurrent.ExecutionContext

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class DisaggBlockManagerEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
  extends ThreadSafeRpcEndpoint with Logging with CrailManager {

  val threshold = thresholdMB * (1000 * 1000)

  logInfo("creating main dir " + rootDir)
  val baseDirExists : Boolean = fs.lookup(rootDir).get() != null

  logInfo("creating main dir " + rootDir)
  if (baseDirExists) {
    fs.delete(rootDir, true).get().syncDir()
  }

  fs.create(rootDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating " + rootDir + " done")
  fs.create(broadcastDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating " + broadcastDir + " done")
  fs.create(shuffleDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating " + shuffleDir + " done")
  fs.create(rddDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating " + rddDir + " done")
  fs.create(tmpDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating " + tmpDir + " done")
  fs.create(metaDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating " + metaDir + " done")
  fs.create(hostsDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
    CrailLocationClass.DEFAULT, true).get().syncDir()
  logInfo("creating main dir done " + rootDir)

  // disagg block size info
  private val disaggBlockInfo: concurrent.Map[BlockId, CrailBlockInfo] =
    new ConcurrentHashMap[BlockId, CrailBlockInfo]().asScala

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val totalSize: AtomicLong = new AtomicLong(0)

  private val lruQueue: mutable.ListBuffer[CrailBlockInfo] =
    new mutable.ListBuffer[CrailBlockInfo]()

  private var lruPointer: Int = 0

  logInfo("DisaggBlockManagerEndpoint up")

  def fileCreated(blockId: BlockId): Boolean = {
    logInfo(s"Disagg endpoint: file created: $blockId")
    if (disaggBlockInfo.contains(blockId)) {
      logInfo(s"tg: Disagg block is already created $blockId")
      false
    } else {
      val blockInfo = new CrailBlockInfo(blockId, getPath(blockId))
      if (disaggBlockInfo.putIfAbsent(blockId, blockInfo).isEmpty) {

        true
      } else {
        false
      }
    }
  }

  def fileRead(blockId: BlockId): Unit = {
    // TODO: file read
    logInfo(s"file read disagg block $blockId")
    if (disaggBlockInfo.get(blockId).isDefined) {
      disaggBlockInfo.get(blockId).get.read = true
    }
  }

  val executor: ExecutorService = Executors.newCachedThreadPool()


  // TODO: which blocks to remove ?
  def discardBlocksIfNecessary(estimateSize: Long): Boolean = {

    logInfo(s"discard block if necessary $estimateSize, pointer: $lruPointer, " +
      s"queueSize: ${lruQueue.size} totalSize: $totalSize / $threshold")


    val removeBlocks: mutable.ListBuffer[BlockId] = new mutable.ListBuffer[BlockId]

    lruQueue.synchronized {

      if (totalSize.get() + estimateSize > threshold) {
        // discard!!
        logInfo(s"Discard blocks.. pointer ${lruPointer} / ${lruQueue.size}")
        val targetDiscardSize: Long = totalSize.get() + estimateSize - threshold
        var totalDiscardSize: Long = 0


        logInfo(s"lruQueue: $lruQueue")

        while (totalDiscardSize < targetDiscardSize) {

          val candidateBlock: CrailBlockInfo = lruQueue(lruPointer)
          if (candidateBlock.writeDone && !candidateBlock.read) {
            // discard!
            totalDiscardSize += candidateBlock.size
            logInfo(s"Discarding ${candidateBlock.bid}..pointer ${lruPointer} / ${lruQueue.size}" +
              s"size $totalDiscardSize / $targetDiscardSize")
            removeBlocks += candidateBlock.bid
            lruQueue.remove(lruPointer)
            // do not move pointer !!
          } else {
            candidateBlock.read = false
            nextLruPointer
            logInfo(s"Skipping block removal... $lruPointer / ${lruQueue.size}, " +
              s"block ${candidateBlock.bid}, wd: ${candidateBlock.writeDone}, " +
              s"r: ${candidateBlock.read} " +
              s" $totalDiscardSize / $targetDiscardSize")
          }

        }
      }

    }

    removeBlocks.foreach { bid =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          logInfo(s"Remove block from worker $bid")
          blockManagerMaster.removeBlockFromWorkers(bid)
        }
      })
    }

    true
  }

  def nextLruPointer: Int = {
    lruPointer += 1
    if (lruPointer >= lruQueue.size) {
      lruPointer = lruPointer % lruQueue.size
    }
    lruPointer
  }

  def fileRemoved(blockId: BlockId): Boolean = {
    logInfo(s"Disagg endpoint: file removed: $blockId")
    val blockInfo = disaggBlockInfo.remove(blockId).get

    lruQueue.synchronized {

      if (lruQueue.contains(blockInfo)) {
        lruQueue -= blockInfo
        lruPointer %= lruQueue.size
      }
    }

    totalSize.addAndGet(-blockInfo.size)

    true
  }

  def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    logInfo(s"Disagg endpoint: file write end: $blockId, size $size")
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
      logWarning(s"No disagg block for writing $blockId")
      throw new RuntimeException(s"no disagg block for writing $blockId")
    } else {
      val v = info.get
      v.size = size
      v.writeDone = true
      totalSize.addAndGet(v.size)

      lruQueue.synchronized {
        lruQueue += v
      }

      logInfo(s"End of disagg file writing $blockId, total: $totalSize")
      true
    }
  }

  def contains(blockId: BlockId): Int = {
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
      logInfo(s"disagg not containing $blockId")
      0
    } else {
      val v = info.get
      if (!v.writeDone) {
        logInfo(s"Waiting for disagg block writing $blockId")
        2
      } else {
        logInfo(s"Disagg endpoint: contains: $blockId")
        1
      }
      /*
      info.synchronized {
        while (!info.writeDone) {
          logInfo(s"Waiting for disagg block writing $blockId")
          return Waiting
          // info.wait()
          // logInfo(s"end of Waiting for disagg block writing $blockId")
        }
      }
      True
      */
    }
  }



  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case FileCreated(blockId) =>
      context.reply(fileCreated(blockId))

    case FileRemoved(blockId) =>
      fileRemoved(blockId)

    case FileRead(blockId) =>
      context.reply(fileRead(blockId))

    case DiscardBlocksIfNecessary(estimateSize) =>
      context.reply(discardBlocksIfNecessary(estimateSize))

    case FileWriteEnd(blockId, size) =>
      fileWriteEnd(blockId, size)

    case Contains(blockId) =>
      context.reply(contains(blockId))

    case GetSize(blockId) =>
      if (disaggBlockInfo.get(blockId).isEmpty) {
        throw new RuntimeException("disagg block is empty.. no size $blockId")
      }
      context.reply(disaggBlockInfo.get(blockId).get.size)

  }
}

class CrailBlockInfo(blockId: BlockId,
                     path: String) {
  val bid = blockId
  var writeDone: Boolean = false
  var size: Long = 0L
  var read: Boolean = true

  override def toString: String = {
    s"<$bid/read:$read>"
  }
}
