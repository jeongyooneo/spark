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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import org.apache.crail.{CrailLocationClass, CrailNodeType, CrailStorageClass}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}
import org.apache.spark.util.ThreadUtils

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}
import scala.concurrent.ExecutionContext


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
abstract class DisaggBlockManagerEndpoint(
                                           override val rpcEnv: RpcEnv,
                                           val isLocal: Boolean,
                                           conf: SparkConf,
                                           listenerBus: LiveListenerBus,
                                           blockManagerMaster: BlockManagerMasterEndpoint,
                                           disaggCapacityMB: Long)
  extends ThreadSafeRpcEndpoint with Logging with CrailManager {

  val threshold: Long = disaggCapacityMB * (1000 * 1000)
  val rddJobDag = blockManagerMaster.rddJobDag
  rddJobDag match {
    case Some(dag) => dag.benefitAnalyzer.setDisaggBlockManagerEndpoint(this)
    case None =>
  }

  val disaggBlockInfo: concurrent.Map[BlockId, CrailBlockInfo] =
    new ConcurrentHashMap[BlockId, CrailBlockInfo]().asScala
  val totalSize: AtomicLong = new AtomicLong(0)
  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)
  val executor: ExecutorService = Executors.newCachedThreadPool()
  // TODO: which blocks to remove ?
  val prevDiscardTime: AtomicLong = new AtomicLong(System.currentTimeMillis())
  val blocksRemovedByMaster: ConcurrentHashMap[BlockId, Boolean] =
    new ConcurrentHashMap[BlockId, Boolean]()
  val blocksSizeToBeCreated: ConcurrentHashMap[BlockId, Long] =
    new ConcurrentHashMap[BlockId, Long]()
  // blockId, size
  val recentlyRemoved: mutable.Map[BlockId, CrailBlockInfo] =
    new mutable.HashMap[BlockId, CrailBlockInfo]()

  logInfo("creating main dir " + rootDir)
  val baseDirExists : Boolean = fs.lookup(rootDir).get() != null
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

  /*
  private val sizePriorityQueue: PriorityQueue[(BlockId, CrailBlockInfo)] =
    new PriorityQueue[(BlockId, CrailBlockInfo)](new Comparator[(BlockId, CrailBlockInfo)] {
      override def compare(o1: (BlockId, CrailBlockInfo), o2: (BlockId, CrailBlockInfo)): Int =
        o2._2.size.compare(o1._2.size)
    })
  */

  logInfo("DisaggBlockManagerEndpoint up")

  // abstract method definition
  // abstract method definition
  // abstract method definition

  def fileCreatedCall(blockInfo: CrailBlockInfo): Unit
  def fileReadCall(blockInfo: CrailBlockInfo): Unit
  def fileRemovedCall(blockInfo: CrailBlockInfo): Unit

  def taskStartedCall(taskId: String): Unit
  def stageCompletedCall(stageId: Int): Unit
  def stageSubmittedCall(stageId: Int): Unit

  def cachingDecision(blockId: BlockId, estimateSize: Long,
                      taskId: String, executorId: String, putDisagg: Boolean): Boolean
  def evictBlocksToIncreaseBenefit(totalCompReduction: Long, totalSize: Long): Unit

  // abstract method definition
  // abstract method definition
  // abstract method definition

  def evictBlocks(removeBlocks: ListBuffer[(BlockId, CrailBlockInfo)]): Unit = synchronized {
    removeBlocks.foreach { b =>
      blocksRemovedByMaster.put(b._1, true)
      totalSize.addAndGet(-b._2.size)

      executor.submit(new Runnable {
        override def run(): Unit = {
          try {
            if (disaggBlockInfo.get(b._1).isDefined) {
              val info = disaggBlockInfo.get(b._1).get

              while (!info.isRemoved) {
                while (info.readCount.get() > 0) {
                  // waiting...
                  Thread.sleep(1000)
                  logInfo(s"Waiting for deleting ${b._1}, count: ${info.readCount}")
                }

                info.synchronized {
                  if (info.readCount.get() == 0 && !info.isRemoved) {
                    info.isRemoved = true
                    recentlyRemoved.put(b._1, info)
                    remove(b._1)
                    logInfo(s"Remove block from worker ${b._1}")
                    blockManagerMaster.removeBlockFromWorkers(b._1)
                  }
                }
              }
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
              throw new RuntimeException(e)
          }
        }
      })
    }
  }

  private def remove(blockId: BlockId): Boolean = {
    val path = getPath(blockId)
    fs.delete(path, false).get()
    logInfo(s"jy: Removed block $blockId from disagg master")
    logInfo(s"Removed block $blockId lookup ${fs.lookup(path).get()}")
    fileRemoved(blockId, false)
    true
  }

  def fileCreated(blockId: BlockId): Boolean = {
    disaggBlockInfo.get(blockId) match {
      case None =>
        logInfo(s"Disagg endpoint: file created: $blockId")
        val blockInfo = new CrailBlockInfo(blockId, getPath(blockId))
        if (disaggBlockInfo.putIfAbsent(blockId, blockInfo).isEmpty) {
          fileCreatedCall(blockInfo)
          true
        } else {
          false
        }
      case Some(v) =>
        v.synchronized {
          if (v.isRemoved) {
            val blockInfo = new CrailBlockInfo(blockId, getPath(blockId))
            if (disaggBlockInfo.putIfAbsent(blockId, blockInfo).isEmpty) {
              fileCreatedCall(blockInfo)
              true
            } else {
              false
            }
          } else {
            false
          }
        }
    }
  }

  def fileReadUnlock(blockId: BlockId): Unit = {
    val info = disaggBlockInfo.get(blockId)
    info.get.readCount.decrementAndGet()
  }

  def fileRead(blockId: BlockId): Int = {
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
      return 0
    } else {
      val v = info.get

      v.synchronized {

        while (!v.writeDone) {
          return 2
          // logInfo(s"Waiting for write done $blockId")
        }

        if (!v.isRemoved) {
          v.readCount.incrementAndGet()
          logInfo(s"file read disagg block $blockId")
          fileReadCall(v)
          return 1
        } else {
          return 0
        }
      }
    }
  }


  def taskStarted(taskId: String): Unit = {
    logInfo(s"Handling task ${taskId} started")
    taskStartedCall(taskId)
  }

  def stageCompleted(stageId: Int): Unit = {
    logInfo(s"Handling stage ${stageId} completed in disagg manager")
    stageCompletedCall(stageId)
  }

  def stageSubmitted(stageId: Int): Unit = {
    logInfo(s"Stage submitted ${stageId}")
    stageSubmittedCall(stageId)
  }

  def timeToRemove(blockCreatedTime: Long, currTime: Long): Boolean = {
    currTime - blockCreatedTime > 7 * 1000
  }

  def fileRemoved(blockId: BlockId, isRemove: Boolean): Boolean = {

    if (isRemove) {
      logInfo(s"Disagg endpoint: file removed from local: $blockId")
      if (disaggBlockInfo.get(blockId).isDefined) {
        val info = disaggBlockInfo.get(blockId).get

        while (!info.isRemoved) {
          while (info.readCount.get() > 0) {
            // waiting...
            Thread.sleep(1000)
            logInfo(s"Waiting for deleting ${blockId}, count: ${info.readCount}")
          }

          info.synchronized {
            if (info.readCount.get() == 0 && !info.isRemoved) {
              val path = getPath(blockId)
              fs.delete(path, false).get()
              info.isRemoved = true
              logInfo(s"jy: Removed block $blockId from disagg local")
              logInfo(s"Removed block $blockId lookup ${fs.lookup(path).get()}")
            }
          }
        }
      }
    }

    disaggBlockInfo.remove(blockId) match {
      case None =>
        if (recentlyRemoved.contains(blockId)) {
          val blockInfo = recentlyRemoved.remove(blockId)
          if (!blocksRemovedByMaster.remove(blockId)) {
            totalSize.addAndGet(-blockInfo.size)
          }
        }
        false
      case Some(blockInfo) =>
        recentlyRemoved.remove(blockId)
        fileRemovedCall(blockInfo)
        if (!blocksRemovedByMaster.remove(blockId)) {
          totalSize.addAndGet(-blockInfo.size)
        }
        true
    }
  }

  def fileWriteEndCall(blockId: BlockId, size: Long): Unit

  def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    // logInfo(s"Disagg endpoint: file write end: $blockId, size $size")
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
      logWarning(s"No disagg block for writing $blockId")
      throw new RuntimeException(s"no disagg block for writing $blockId")
    } else {
      val v = info.get
      v.size = size
      v.createdTime = System.currentTimeMillis()
      v.writeDone = true

      if (blocksSizeToBeCreated.containsKey(blockId)) {
        val estimateSize = blocksSizeToBeCreated.remove(blockId)
        totalSize.addAndGet(v.size - estimateSize)
      } else {
        throw new RuntimeException(s"No created block $blockId")
      }

      /*
      lruQueue.synchronized {
        lruQueue.append(v)
      }

      sizePriorityQueue.synchronized {
        sizePriorityQueue.add((blockId, v))
      }
      */

      logInfo(s"Storing file writing $blockId, size $size, total: $totalSize")
      fileWriteEndCall(blockId, size)
      true
    }
  }

  def contains(blockId: BlockId): Int = {
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
       // logInfo(s"disagg not containing $blockId")
      0 } else {
      val v = info.get
      if (!v.writeDone) {
        logInfo(s"Waiting for disagg block writing $blockId")
        2
      } else {
        // logInfo(s"Disagg endpoint: contains: $blockId")
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

  def localEviction(blockId: Option[BlockId], executorId: String, size: Long): List[BlockId] = {
    List.empty
  }

  def localEvictionDone(blockId: BlockId): Unit = {

  }

  def localEvictionFail(blockId: BlockId, executorId: String, size: Long): Unit = {

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case FileCreated(blockId) =>
      context.reply(fileCreated(blockId))

    case FileRemoved(blockId, remove) =>
      fileRemoved(blockId, remove)

    case FileRead(blockId) =>
      context.reply(fileRead(blockId))

    case FileReadUnlock(blockId) =>
      context.reply(fileReadUnlock(blockId))

    case DiscardBlocksIfNecessary(estimateSize) =>
      throw new RuntimeException("not supported")

    case StoreBlockOrNot(blockId, estimateSize, taskId, executorId, putDisagg) =>
      context.reply(cachingDecision(blockId, estimateSize, taskId, executorId, putDisagg))

    case FileWriteEnd(blockId, size) =>
      fileWriteEnd(blockId, size)

    case Contains(blockId) =>
      context.reply(contains(blockId))

    case GetSize(blockId) =>
      if (disaggBlockInfo.get(blockId).isEmpty) {
        logWarning(s"disagg block is empty.. no size $blockId")
        context.reply(0)
      } else if (!disaggBlockInfo.get(blockId).get.writeDone) {
        throw new RuntimeException(s"disagg block size is 0.. not write done $blockId")
      } else {
        context.reply(disaggBlockInfo.get(blockId).get.size)
      }

    case LocalEviction(blockId, executorId, size) =>
      context.reply(localEviction(blockId, executorId, size))
    case EvictionFail(blockId, executorId, size) =>
      localEvictionFail(blockId, executorId, size)
    case LocalEvictionDone(blockId) =>
      localEvictionDone(blockId)
  }
}

class CrailBlockInfo(blockId: BlockId,
                     path: String) {
  val bid = blockId
  var writeDone: Boolean = false
  var size: Long = 0L
  var read: Boolean = true
  val readCount: AtomicInteger = new AtomicInteger()
  var isRemoved = false
  var createdTime = System.currentTimeMillis()
  var refTime = System.currentTimeMillis()
  var refCnt: AtomicInteger = new AtomicInteger()
  var nectarCost: Long = 0L

  override def toString: String = {
    s"<$bid/read:$read>"
  }
}

object DisaggBlockManagerEndpoint {

  def apply(rpcEnv: RpcEnv,
            isLocal: Boolean,
            conf: SparkConf,
            listenerBus: LiveListenerBus,
            blockManagerMaster: BlockManagerMasterEndpoint,
            thresholdMB: Long): DisaggBlockManagerEndpoint = {
    val policy = conf.get("spark.disagg.evictpolicy", "None")

    if (policy.equals("LRU")) {
      new LRUEvictionManagerEndpoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else if (policy.equals("DRDD")) {
      new RDDCostBasedEvictionEndpoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else if (policy.equals("Autosizing")) {
      new RDDAutosizingEvictionEndpoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else if (policy.equals("Nectar")) {
       new NectarEvictionEndpoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else if (policy.equals("Local")) {
      new RDDLocalMemoryPolicyPoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else if (policy.equals("None")) {
      new NoEvictionManagerEndpoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else if (policy.equals("Local-Disagg")) {
      new RDDLocalDisaggMemoryPolicyPoint(rpcEnv, isLocal,
        conf, listenerBus, blockManagerMaster, thresholdMB)
    } else {
      throw new RuntimeException("Not supported eviction " + policy)
    }
  }
}
