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

import java.util.{Comparator, PriorityQueue}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

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

  val threshold: Long = thresholdMB * (1000 * 1000)

  logInfo("creating main dir " + rootDir)
  val baseDirExists : Boolean = fs.lookup(rootDir).get() != null

  logInfo("creating main dir " + rootDir)
  if (baseDirExists) {
    fs.delete(rootDir, true).get().syncDir()
  }

  /*
  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(conf))

  val dest = new File("/", dagPath)
  val destPath = new Path(new URI(dest))

  val inputStream = fileSystem.open(destPath)

  inputStream.readFully()
  */

  val rddJobDag = blockManagerMaster.rddJobDag

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

  private val sizePriorityQueue: PriorityQueue[(BlockId, CrailBlockInfo)] =
    new PriorityQueue[(BlockId, CrailBlockInfo)](new Comparator[(BlockId, CrailBlockInfo)] {
      override def compare(o1: (BlockId, CrailBlockInfo), o2: (BlockId, CrailBlockInfo)): Int =
        o2._2.size.compare(o1._2.size)
    })

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
      // logInfo(s"tg: Disagg block is already created $blockId")
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

      while (!v.writeDone) {
        return 2
        // logInfo(s"Waiting for write done $blockId")
      }

      v.synchronized {
        if (!v.isRemoved) {
          v.readCount.incrementAndGet()
          return 1
        } else {
          return 0
        }
      }
    }

    // TODO: file read
    // logInfo(s"file read disagg block $blockId")

    // disable file read
    /*
    if (disaggBlockInfo.get(blockId).isDefined) {
      val info = disaggBlockInfo.get(blockId).get
      info.read = true

      lruQueue.synchronized {
        lruQueue -= info

        // mru
        lruQueue.prepend(info)

        // lru
        // lruQueue.append(info)
      }
    }
    */
  }

  val executor: ExecutorService = Executors.newCachedThreadPool()


  // TODO: which blocks to remove ?

  val prevDiscardTime: AtomicLong = new AtomicLong(System.currentTimeMillis())

  val blocksRemovedByMaster: ConcurrentHashMap[BlockId, Boolean] =
    new ConcurrentHashMap[BlockId, Boolean]()

  val blocksSizeToBeCreated: ConcurrentHashMap[BlockId, Long] =
    new ConcurrentHashMap[BlockId, Long]()


  val prevCreatedBlocks: ConcurrentHashMap[BlockId, Boolean] =
    new ConcurrentHashMap[BlockId, Boolean]()

  def taskStarted(taskId: String): Unit = {
    logInfo(s"Handling task ${taskId} started")
    rddJobDag.get.taskStarted(taskId)
  }

  def stageCompleted(stageId: Int): Unit = {
    logInfo(s"Handling stage ${stageId} completed in disagg manager")
    rddJobDag.get.removeCompletedStageNode(stageId)
  }

  def stageSubmitted(stageId: Int): Unit = {
    logInfo(s"Stage submitted ${stageId}")
    rddJobDag.get.stageSubmitted(stageId)
  }


  private def timeToRemove(blockCreatedTime: Long, currTime: Long): Boolean = {
    currTime - blockCreatedTime > 8 * 1000
  }

  val recentlyRemoved: mutable.Set[BlockId] = new mutable.HashSet[BlockId]()

  def storeBlockOrNot(blockId: BlockId, estimateSize: Long): Boolean = {

    val prevTime = prevDiscardTime.get()

    if (!prevCreatedBlocks.containsKey(blockId)) {
      prevCreatedBlocks.put(blockId, true)
    }

    rddJobDag.get.blockCreated(blockId)

    /*
    if (prevDiscardedBlocks.containsKey(blockId)) {
      logInfo(s"Discard $blockId because it is already discarded")
      return false
    }
    */

    val putCost = rddJobDag.get.calculateCostToBeStored(blockId, System.currentTimeMillis())

    synchronized {
      if (disaggBlockInfo.contains(blockId)) {
        return false
      }

      if (totalSize.get() + estimateSize < threshold
        || System.currentTimeMillis() - prevTime < 50) {
        logInfo(s"Storing $blockId, cost: $putCost, " +
          s"size $estimateSize / $totalSize, threshold: $threshold")

        blocksSizeToBeCreated.put(blockId, estimateSize)
        totalSize.addAndGet(estimateSize)
        rddJobDag.get.storingBlock(blockId)

        return true
      }

      // discard!!
      var totalCost = 0L
      var totalDiscardSize = 0L

      val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
        new mutable.ListBuffer[(BlockId, CrailBlockInfo)]

      if (prevDiscardTime.compareAndSet(prevTime, System.currentTimeMillis())) {

        /*
        rddJobDag.get.sortedBlockCost match {
          case None =>
            None
          case Some(l) =>
            val iterator = l.iterator

            val removalSize = Math.max(2 * 1024 * 1024 * 1024L,
              totalSize.get() + estimateSize - threshold)

            val currTime = System.currentTimeMillis()
            while (iterator.hasNext && totalDiscardSize < removalSize) {
              val (bid, discardCost) = iterator.next()
              disaggBlockInfo.get(bid) match {
                case None =>
                  // do nothing
                case Some(blockInfo) =>
                  if (discardCost <= 0 && timeToRemove(blockInfo.createdTime, currTime)
                          && !recentlyRemoved.contains(bid)) {

                    // gc !!
                    totalDiscardSize += blockInfo.size
                    removeBlocks.append((bid, blockInfo))
                    logInfo(s"Try to remove: Cost: $totalCost/$putCost, " +
                      s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                  } else if (totalCost + discardCost < putCost
                    && timeToRemove(blockInfo.createdTime, currTime)
                    && !recentlyRemoved.contains(bid)) {
                    totalCost += discardCost
                    totalDiscardSize += blockInfo.size
                    removeBlocks.append((bid, blockInfo))
                    logInfo(s"Try to remove: Cost: $totalCost/$putCost, " +
                      s"size: $totalDiscardSize/$removalSize, remove block: $bid")
                  }
              }
            }
        }
        */

        sizePriorityQueue.synchronized {
          val iterator = sizePriorityQueue.iterator

          val removalSize = Math.max(2 * 1024 * 1024 * 1024L,
            totalSize.get() + estimateSize - threshold)

          val currTime = System.currentTimeMillis()
          while (iterator.hasNext && totalDiscardSize < removalSize) {
            val (bid, blockInfo) = iterator.next()

            val discardCost = rddJobDag.get.getCost(bid)

            if (totalCost + discardCost < putCost
              && timeToRemove(blockInfo.createdTime, currTime)) {
              totalCost += discardCost
              totalDiscardSize += blockInfo.size
              removeBlocks.append((bid, blockInfo))
              iterator.remove()
              logInfo(s"Try to remove: Cost: $totalCost/$putCost, " +
                s"size: $totalDiscardSize/$removalSize, remove block: $bid")
            }
          }
        }
      }

      removeBlocks.foreach { t =>

        blocksRemovedByMaster.put(t._1, true)
        totalSize.addAndGet(-t._2.size)

        rddJobDag.get.removingBlock(blockId)

        executor.submit(new Runnable {
          override def run(): Unit = {
            if (disaggBlockInfo.get(t._1).isDefined) {
              val info = disaggBlockInfo.get(t._1).get

              while (!info.isRemoved) {
                while (info.readCount.get() > 0) {
                  // waiting...
                  Thread.sleep(1000)
                  logInfo(s"Waiting for deleting ${t._1}, count: ${info.readCount}")
                }

                info.synchronized {
                  if (info.readCount.get() == 0) {
                    info.isRemoved = true
                    recentlyRemoved.add(t._1)

                    logInfo(s"Remove block from worker ${t._1}")
                    blockManagerMaster.removeBlockFromWorkers(t._1)

                  }
                }
              }
            }
          }
        })
      }

      if (totalDiscardSize < estimateSize || putCost < 2000) {
        // it means that the discarding cost is greater than putting block
        // so, we do not store the block
        logInfo(s"Discarding $blockId, discardingCost: $totalCost, " +
          s"discardingSize: $totalDiscardSize/$estimateSize")

        /*
        sizePriorityQueue.synchronized {
          removeBlocks.foreach { t =>
            sizePriorityQueue.add(t)
          }
        }
        */

        rddJobDag.get.setCreatedTimeBlock(blockId)

        false
      } else {
        logInfo(s"Storing $blockId, size $estimateSize / $totalSize, threshold: $threshold")
        blocksSizeToBeCreated.put(blockId, estimateSize)
        totalSize.addAndGet(estimateSize)
        rddJobDag.get.storingBlock(blockId)

        true
      }
    }
  }

  def discardBlocksIfNecessary(estimateSize: Long): Boolean = {

    val disaggTotalSize = blockManagerMaster.totalDisaggSize.get()
    logInfo(s"discard block if necessary $estimateSize, pointer: $lruPointer, " +
      s"queueSize: ${lruQueue.size} totalSize: $disaggTotalSize / $totalSize / $threshold")


    val removeBlocks: mutable.ListBuffer[BlockId] = new mutable.ListBuffer[BlockId]
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

          while (totalDiscardSize < targetDiscardSize && lruQueue.nonEmpty && cnt < lruSize) {
            val candidateBlock: CrailBlockInfo = lruQueue.head

            if (candidateBlock.bid.name.startsWith("rdd_2_")) {
              // skip..
              lruQueue -= candidateBlock
              lruQueue.append(candidateBlock)
            } else {
              totalDiscardSize += candidateBlock.size
              logInfo(s"Discarding ${candidateBlock.bid}.." +
                s"pointer ${lruPointer} / ${lruQueue.size}" +
                s"size $totalDiscardSize / $targetDiscardSize")
              removeBlocks += candidateBlock.bid

              lruQueue -= candidateBlock
            }

            cnt += 1
          }
        }

          /*
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
        */
      }
    }


    removeBlocks.foreach { bid =>
      executor.submit(new Runnable {
        override def run(): Unit = {
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
    // logInfo(s"Disagg endpoint: file removed: $blockId")
    val blockInfo = disaggBlockInfo.remove(blockId).get

    recentlyRemoved.remove(blockId)

    rddJobDag.get.removingBlock(blockId)

    lruQueue.synchronized {

      if (lruQueue.contains(blockInfo)) {
        lruQueue -= blockInfo
        lruPointer %= lruQueue.size
      }
    }

    /*
    sizePriorityQueue.synchronized {
      val iterator = sizePriorityQueue.iterator()
      var done = false
      while (iterator.hasNext && !done) {
        val (bid, info) = iterator.next()
        if (bid.equals(blockId)) {
          iterator.remove()
          done = true
        }
      }
    }
    */

    if (!blocksRemovedByMaster.remove(blockId)) {
      totalSize.addAndGet(-blockInfo.size)
    }

    true
  }

  def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    // logInfo(s"Disagg endpoint: file write end: $blockId, size $size")
    val info = disaggBlockInfo.get(blockId)


    if (info.isEmpty) {
      logWarning(s"No disagg block for writing $blockId")
      throw new RuntimeException(s"no disagg block for writing $blockId")
    } else {


      val v = info.get
      v.size = size
      v.writeDone = true

      if (blocksSizeToBeCreated.containsKey(blockId)) {
        val estimateSize = blocksSizeToBeCreated.remove(blockId)
        totalSize.addAndGet(v.size - estimateSize)
      } else {
        throw new RuntimeException(s"No created block $blockId")
      }

      lruQueue.synchronized {
        lruQueue.append(v)
      }

      sizePriorityQueue.synchronized {
        sizePriorityQueue.add((blockId, v))
      }

      logInfo(s"Storing file writing $blockId, total: $totalSize")
      true
    }
  }

  def contains(blockId: BlockId): Int = {
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
       // logInfo(s"disagg not containing $blockId")
      0
    } else {
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



  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case FileCreated(blockId) =>
      context.reply(fileCreated(blockId))

    case FileRemoved(blockId) =>
      fileRemoved(blockId)

    case FileRead(blockId) =>
      context.reply(fileRead(blockId))

    case FileReadUnlock(blockId) =>
      context.reply(fileReadUnlock(blockId))

    case DiscardBlocksIfNecessary(estimateSize) =>
      context.reply(discardBlocksIfNecessary(estimateSize))

    case StoreBlockOrNot(blockId, estimateSize, taskId) =>
      context.reply(storeBlockOrNot(blockId, estimateSize))

    case FileWriteEnd(blockId, size) =>
      fileWriteEnd(blockId, size)

    case Contains(blockId) =>
      context.reply(contains(blockId))

    case GetSize(blockId) =>
      if (disaggBlockInfo.get(blockId).isEmpty) {
        throw new RuntimeException(s"disagg block is empty.. no size $blockId")
      } else if (!disaggBlockInfo.get(blockId).get.writeDone) {
        throw new RuntimeException(s"disagg block size is 0.. not write done $blockId")
      } else {
        context.reply(disaggBlockInfo.get(blockId).get.size)
      }
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
  val createdTime = System.currentTimeMillis()

  override def toString: String = {
    s"<$bid/read:$read>"
  }
}
