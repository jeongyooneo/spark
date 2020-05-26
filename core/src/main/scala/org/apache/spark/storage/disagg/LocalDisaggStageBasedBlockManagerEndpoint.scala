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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReadWriteLock, StampedLock}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMasterEndpoint}
import org.apache.spark.util.ThreadUtils

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}
import scala.concurrent.ExecutionContext

/**
 */
private[spark] class LocalDisaggStageBasedBlockManagerEndpoint(
                          override val rpcEnv: RpcEnv,
                          val isLocal: Boolean,
                          conf: SparkConf,
                          listenerBus: LiveListenerBus,
                          blockManagerMaster: BlockManagerMasterEndpoint,
                          disaggCapacityMB: Long,
                          val costAnalyzer: CostAnalyzer,
                          val metricTracker: MetricTracker,
                          val cachingPolicy: CachingPolicy,
                          val evictionPolicy: EvictionPolicy,
                          val rddJobDag: Option[RDDJobDag])
  extends DisaggBlockManagerEndpoint {

  blockManagerMaster.setDisaggBlockManager(this)

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  val autounpersist = conf.get(BlazeParameters.AUTOUNPERSIST)
  val executor: ExecutorService = Executors.newCachedThreadPool()
  val disableLocalCaching = conf.get(BlazeParameters.DISABLE_LOCAL_CACHING)

  val recentlyBlockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()
  val localExecutorLockMap = new ConcurrentHashMap[String, Object]().asScala
  private val disaggBlockLockMap = new ConcurrentHashMap[BlockId, ReadWriteLock].asScala
  private val stageJobMap = new mutable.HashMap[Int, Int]()

  val scheduler = Executors.newSingleThreadScheduledExecutor()
  val task = new Runnable {
    def run(): Unit = {
      try {
        costAnalyzer.update
        val map = blockManagerMaster.blockManagerInfo.map {
          entry => val executor = entry._1.executorId
            val blocks = entry._2.blocks.filter(p => p._1.isRDD).keySet
            (executor, blocks)
        }.toMap

        /*
        metricTracker.localStoredBlocksMap.asScala.foreach {
          entry => {
            val v1 = map(entry._1)
            val v2 = entry._2
            if (!v1.equals(v2)) {
              logInfo(s"Block mismatch executor ${entry._1}: local: ${v2} / blockInfo: ${v1}")
            }
          }
        }
        */

        logInfo(s"Total disagg: ${metricTracker.disaggTotalSize.get() / (1024 * 1024)}" +
          s" / ${disaggThreshold / 1024 / 1024}")



        // logInfo(s"Disagg blocks: ${disaggBlockInfo.keySet}")
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw new RuntimeException(e)
      }
    }
  }
  scheduler.scheduleAtFixedRate(task, 2, 2, TimeUnit.SECONDS)

  override def onStop(): Unit = {
    scheduler.shutdownNow()
  }

  /**
   * Parameters for disaggregation caching.
   */

  // disagg capacity
  val disaggThreshold: Long = disaggCapacityMB * (1024 * 1024)

  // blocks stored in disagg
  val disaggBlockInfo: concurrent.Map[BlockId, CrailBlockInfo] =
    new ConcurrentHashMap[BlockId, CrailBlockInfo]().asScala

  // blocks recently removed from disagg
  val recentlyRemoved: mutable.Map[BlockId, Boolean] =
    new mutable.HashMap[BlockId, Boolean]()

  /**
   * Parameters for local caching.
   */
  // val executorCacheMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  val recentlyEvictFailBlocksFromLocal: mutable.Map[BlockId, Long] =
    new mutable.HashMap[BlockId, Long]().withDefaultValue(0L)

  // Public methods
  def removeExecutor(executorId: String): Unit = {
    logInfo(s"Remove executor and release lock for $executorId")

    localExecutorLockMap(executorId).synchronized {
      if (executorWriteLockCount.contains(executorId)) {
        val set = executorWriteLockCount.remove(executorId).get
        set.foreach {
          bid => disaggBlockLockMap(bid).writeLock().unlock()
            if (!disaggBlockInfo(bid).writeDone) {
              disaggBlockInfo.remove(bid)
            }
        }
      }

      if (executorReadLockCount.contains(executorId)) {
        val blockCounterMap = executorReadLockCount.remove(executorId).get.asScala
        blockCounterMap.foreach {
          entry => val bid = entry._1
            val cnt = entry._2
            for (i <- 1 to cnt.get()) {
              disaggBlockLockMap(bid).readLock().unlock()
            }
        }
      }

      // Remove local info
      metricTracker.removeExecutorBlocks(executorId)
    }
  }

  def isRddCache(rddId: Int): Boolean = {
    val cache = cachingPolicy.isRDDNodeCached(rddId)
    if (cache) {
      BlazeLogger.logCachingDecision(rddId)
    }
    cache
  }

  def taskStarted(taskId: String): Unit = {
    logInfo(s"Handling task ${taskId} started")
    metricTracker.taskStarted(taskId)
  }

  private var prevCleanupTime = System.currentTimeMillis()
  private val fullyProfiled = conf.get(BlazeParameters.FULLY_PROFILED)
  private val currJob = new AtomicInteger(-1)


  override def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (disaggBlockInfo.contains(blockId)) {
      val executorId = disaggBlockInfo(blockId).executorId
      List(blockManagerMaster.executorBlockManagerMap(executorId)).toSeq
    } else {
      Seq.empty
    }
  }

  def stageCompleted(stageId: Int): Unit = {
    logInfo(s"Handling stage ${stageId} completed in disagg manager")
    metricTracker.stageCompleted(stageId)

    if (autounpersist) {
      autounpersist.synchronized {

        if (System.currentTimeMillis() - prevCleanupTime >= 10000) {
          // removeDupRDDsFromDisagg

          // unpersist rdds
          val zeroRDDs = costAnalyzer.findZeroCostRDDs
            .filter {
              p =>
                val rddNode = rddJobDag.get.getRDDNode(p)
                val lastStage = rddNode.getStages.max

                logInfo(s"StateCompleted ${stageId} LastJob of RDD " +
                  s"${rddNode.rddId}: stage $lastStage, " +
                  s"currJob: ${currJob.get()}, jobMap: ${stageJobMap}")

                if (stageJobMap.contains(lastStage)) {
                  logInfo(s"StateCompleted ${stageId} LastJob of RDD " +
                    s"${rddNode.rddId}: stage $lastStage, " +
                    s"jobId: ${stageJobMap(lastStage)}, " +
                    s"currJob: ${currJob.get()}, jobMap: ${stageJobMap}")

                  if (!fullyProfiled) {
                    currJob.get() > stageJobMap(lastStage)
                  } else {
                    // stageId >= lastStage
                    true
                  }
                } else {
                  if (!fullyProfiled) {
                    false
                  } else {
                    // stageId >= lastStage
                    true
                  }
                }
            }

          zeroRDDs.foreach {
            rdd =>
              logInfo(s"Remove zero cost rdd $rdd from memory")
              // remove from local executors
              blockManagerMaster.removeRdd(rdd)
            // remove local info
          }

          // Here, we remove RDDs from local and disagg
          removeRddsFromLocal(zeroRDDs)
          removeRddsFromDisagg(zeroRDDs)

          prevCleanupTime = System.currentTimeMillis()
        }
      }
    }
  }


  def stageSubmitted(stageId: Int, jobId: Int): Unit = synchronized {
    stageJobMap.put(stageId, jobId)
    logInfo(s"Stage submitted ${stageId}, jobId: $jobId, jobMap: $stageJobMap")
    currJob.set(jobId)
    metricTracker.stageSubmitted(stageId)
  }

  def removeFromLocal(blockId: BlockId, executorId: String): Unit = {
    metricTracker.removeExecutorBlock(blockId, executorId)
  }

  // Private methods

  private def addToLocal(blockId: BlockId, executorId: String, estimateSize: Long): Unit = {
    metricTracker.addExecutorBlock(blockId, executorId, estimateSize)
  }

  private def cachingFail(blockId: BlockId, estimateSize: Long,
                              executorId: String,
                              putDisagg: Boolean, localFull: Boolean): Unit = {
    BlazeLogger.cachingFailure(blockId, executorId, estimateSize)
    removeFromLocal(blockId, executorId)
  }

  private val disaggFirst = conf.get(BlazeParameters.DISAGG_FIRST)

  private def cachingDecision(blockId: BlockId, estimateSize: Long,
                              executorId: String,
                              putDisagg: Boolean, localFull: Boolean): Boolean = {

    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    // logInfo(s"Caching decision call " +
    //  s"$blockId, $estimateSize, $executorId, $putDisagg, $localFull")

    val t = System.currentTimeMillis()
    metricTracker.blockCreatedTimeMap.putIfAbsent(blockId, t)
    metricTracker.localBlockSizeHistoryMap.putIfAbsent(blockId, estimateSize)
    metricTracker.localStoredBlocksHistoryMap
      .putIfAbsent(executorId, ConcurrentHashMap.newKeySet[BlockId].asScala)
    metricTracker.localStoredBlocksHistoryMap.get(executorId).add(blockId)
    metricTracker.recentlyBlockCreatedTimeMap.put(blockId, t)
    val storingCost = costAnalyzer.compDisaggCost(blockId)

    if (estimateSize == 0) {
      logInfo(s"RDD estimation size zero $blockId")
    }

    if (!putDisagg) {
      if (disableLocalCaching) {
        // Do not cache blocks in local
        // just return false
        BlazeLogger.discardLocal(blockId, executorId,
          storingCost.reduction, storingCost.disaggCost, estimateSize, "onlyDisagg")
        recentlyRecachedBlocks.remove(blockId)
        false
      } else {
        if (localFull) {
          if (disaggThreshold < estimateSize) {
            val l = costAnalyzer.sortedBlockByCompCostInLocal.get()(executorId)

            if (l.isEmpty) {
              BlazeLogger.discardLocal(blockId, executorId,
                storingCost.reduction, storingCost.disaggCost, estimateSize, "Empty?!?!")
              recentlyRecachedBlocks.remove(blockId)
              false
            } else {
              if (l.head.reduction > storingCost.reduction) {
                BlazeLogger.discardLocal(blockId, executorId,
                  storingCost.reduction, storingCost.disaggCost, estimateSize, "Minimum")
                recentlyRecachedBlocks.remove(blockId)
                false
              } else {

                if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                  BlazeLogger.recacheDisaggToLocal(blockId, executorId)
                }

                addToLocal(blockId, executorId, estimateSize)
                BlazeLogger.logLocalCaching(blockId, executorId,
                  estimateSize, storingCost.reduction, storingCost.disaggCost, "1")
                true
              }
            }
          } else {
            if (evictionPolicy
              .decisionLocalEviction(storingCost, executorId, blockId, estimateSize)) {
              addToLocal(blockId, executorId, estimateSize)
              BlazeLogger.logLocalCaching(blockId, executorId,
                estimateSize, storingCost.reduction, storingCost.disaggCost, "2")

              if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                BlazeLogger.recacheDisaggToLocal(blockId, executorId)
              }

              true
            } else {
              BlazeLogger.discardLocal(blockId, executorId,
                storingCost.reduction, storingCost.disaggCost, estimateSize, s"$estimateSize")
              recentlyRecachedBlocks.remove(blockId)
              false
            }
          }
        } else {
          // local caching
          // put until threshold
          addToLocal(blockId, executorId, estimateSize)
          BlazeLogger.logLocalCaching(blockId, executorId,
            estimateSize, storingCost.reduction, storingCost.disaggCost, "3")

          if (recentlyRecachedBlocks.remove(blockId).isDefined) {
            BlazeLogger.recacheDisaggToLocal(blockId, executorId)
          }

          true
        }
      }
    } else {
      disaggDecision(blockId, estimateSize, executorId, true)
    }
  }

  private def localEviction(blockId: Option[BlockId],
                            executorId: String, evictionSize: Long,
                            prevEvicted: Set[BlockId]): List[BlockId] = {

    val evictionList: mutable.ListBuffer[BlockId] = new ListBuffer[BlockId]
    val blockManagerId = blockManagerMaster.executorBlockManagerMap.get(executorId).get
    val blockManagerInfo = blockManagerMaster.blockManagerInfo(blockManagerId)
    val currTime = System.currentTimeMillis()
    var sizeSum = 0L

    if (!blockId.isDefined || (blockId.isDefined && !blockId.get.isRDD)) {
      // we should evict blocks to free space for execution !!
      logInfo(s"Evict to free space at executor " +
        s"$executorId, blockId: $blockId, size: $evictionSize")
      val l = costAnalyzer.sortedBlockByCompCostInLocal.get()(executorId)
      l.foreach {
        discardingBlock => {
          if (!prevEvicted.contains(discardingBlock.blockId)) {
            val elapsed = currTime -
              recentlyEvictFailBlocksFromLocal.getOrElse(discardingBlock.blockId, 0L)
            val createdTime = metricTracker
              .recentlyBlockCreatedTimeMap.get(discardingBlock.blockId)
            if (elapsed > 5000 && timeToRemove(createdTime, System.currentTimeMillis())) {
              recentlyEvictFailBlocksFromLocal.remove(discardingBlock.blockId)
              if (blockManagerInfo.blocks.contains(discardingBlock.blockId)) {
                sizeSum += blockManagerInfo.blocks(discardingBlock.blockId).memSize
                evictionList.append(discardingBlock.blockId)
              }

              if (sizeSum > evictionSize) {
                evictionList.foreach {
                  bid =>
                    removeFromLocal(bid, executorId)
                }
                return evictionList.toList
              }
            }
          }
        }
      }
      return evictionList.toList
    }

    val bid = blockId.get
    val storingCost = costAnalyzer.compDisaggCost(bid)

    evictionPolicy.selectEvictFromLocal(storingCost, executorId, blockId.get) {
      iter =>
        if (iter.isEmpty) {
          logWarning(s"Low comp disagg block is empty " +
            s"for evicting $blockId, size $evictionSize")
          List.empty
        } else {
          var sum = 0.0

          iter.foreach {
            discardingBlock => {
              if (!prevEvicted.contains(discardingBlock.blockId)
                && discardingBlock.blockId != bid
                && discardingBlock.reduction <= storingCost.reduction) {
                val elapsed = currTime -
                  recentlyEvictFailBlocksFromLocal.getOrElse(discardingBlock.blockId, 0L)
                val createdTime = metricTracker
                  .recentlyBlockCreatedTimeMap.get(discardingBlock.blockId)
                if (elapsed > 5000 && timeToRemove(createdTime, System.currentTimeMillis())) {
                  recentlyEvictFailBlocksFromLocal.remove(discardingBlock.blockId)
                  if (blockManagerInfo.blocks.contains(discardingBlock.blockId) &&
                    sum <= storingCost.reduction) {
                    sum += discardingBlock.reduction
                    sizeSum += blockManagerInfo.blocks(discardingBlock.blockId).memSize
                    evictionList.append(discardingBlock.blockId)
                  }
                }
              }
            }

              if (sizeSum >= evictionSize + 5 * 1024 * 1024) {
                logInfo(s"CostSum: $sum, block: $blockId")
                evictionList.foreach {
                  bid =>
                    removeFromLocal(bid, executorId)
                }
                return evictionList.toList
              }
          }

          if (sizeSum >= evictionSize) {
            logInfo(s"CostSum: $sum, block: $blockId")
            evictionList.foreach {
              bid =>
                removeFromLocal(bid, executorId)
            }
            return evictionList.toList
          } else {
            logWarning(s"Size sum $sizeSum < eviction Size $evictionSize, " +
              s"for caching ${blockId} selected list: $evictionList")
            List.empty
          }
        }
    }
  }

  private def localEvictionFail(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"Local eviction failed: $blockId, $executorId")
    recentlyEvictFailBlocksFromLocal.put(blockId, System.currentTimeMillis())
    addToLocal(blockId, executorId, metricTracker.localBlockSizeHistoryMap.get(blockId))
  }

  private def localEvictionDone(blockId: BlockId, executorId: String): Unit = {
    val cost = costAnalyzer.compDisaggCost(blockId)
    BlazeLogger.evictLocal(
      blockId, executorId, cost.reduction, cost.disaggCost, metricTracker.getBlockSize(blockId))
    // removeFromLocal(blockId, executorId)
  }


  private val recentlyRecachedBlocks = new ConcurrentHashMap[BlockId, Boolean]().asScala

  def cacheDisaggDataInMemory(blockId: BlockId, size: Long,
                              executorId: String,
                              enoughSpace: Boolean): Boolean = {

    // logInfo(s"Maybe cache into memory " +
    //  s"$blockId, $size, $executorId, $enoughSpace")

    /*
    val costForStoredBlock = costAnalyzer.compDisaggCost(blockId)

    if (evictionPolicy.decisionPromote(costForStoredBlock, executorId, blockId, size)) {
      // we store this rdd and evict others
      BlazeLogger.recacheDisaggToLocal(blockId, executorId)
      true
    } else {
      false
    }
    */


    // We only recache the block if the block was stored in the executor
    if (disableLocalCaching || metricTracker.storedBlockInLocal(blockId) ) {
      return false
    }

    if (recentlyRecachedBlocks.contains(blockId)) {
      return false
    }

    if (enoughSpace) {
      if (recentlyRecachedBlocks.putIfAbsent(blockId, true).isEmpty) {
        return true
      } else {
        return false
      }
    }

    val costForStoredBlock = costAnalyzer.compDisaggCost(blockId)

    if (evictionPolicy.decisionPromote(costForStoredBlock, executorId, blockId, size)
      && recentlyRecachedBlocks.putIfAbsent(blockId, true).isEmpty) {
      // we store this rdd and evict others
      true
    } else {
      false
    }
  }

  private def addToDisagg(blockId: BlockId,
                          estimateBlockSize: Long,
                          cost: CompDisaggCost): Unit = {
    metricTracker.addBlockInDisagg(blockId, estimateBlockSize)
  }

  private def disaggDecision(blockId: BlockId,
                             estimateSize: Long,
                             executorId: String,
                             putDisagg: Boolean): Boolean = {

    val cost = costAnalyzer.compDisaggCost(blockId)
    val estimateBlockSize = DisaggUtils.calculateDisaggBlockSize(estimateSize)
    val rddId = blockId.asRDDId.get.rddId

    // val t = System.currentTimeMillis()
    // metricTracker.blockCreatedTimeMap.putIfAbsent(blockId, t)
    // metricTracker.recentlyBlockCreatedTimeMap.put(blockId, t)

    val removeBlocks: mutable.ListBuffer[(BlockId, CrailBlockInfo)] =
      new mutable.ListBuffer[(BlockId, CrailBlockInfo)]
    val rmBlocks: mutable.ListBuffer[CompDisaggCost] =
      new mutable.ListBuffer[CompDisaggCost]

    var totalDiscardSize = 0L
    val removalSize = Math.max(estimateBlockSize, metricTracker.disaggTotalSize.get()
      + estimateBlockSize - disaggThreshold + 2 * (1024 * 1024))

    if (cost.reduction <= 0) {
      BlazeLogger.discardDisagg(
        blockId, cost.reduction, cost.disaggCost, estimateSize, "by master0")
      return false
    }

    disaggBlockLockMap.synchronized {
      if (disaggBlockInfo.contains(blockId)) {
        BlazeLogger.discardDisagg(
          blockId, cost.reduction, cost.disaggCost, estimateSize, "by master1")
        return false
      }


      // If we have enough space in disagg memory, cache it
      if (metricTracker.disaggTotalSize.get() + estimateBlockSize < disaggThreshold) {
        BlazeLogger.logDisaggCaching(blockId, estimateBlockSize, cost.reduction, cost.disaggCost)
        addToDisagg(blockId, estimateBlockSize, cost)

        // We create info here and lock here
        val blockInfo = new CrailBlockInfo(blockId, executorId, getPath(blockId))
        disaggBlockInfo.put(blockId, blockInfo)
        // We should unlock it after file is created
        if (blockWriteLock(blockId, executorId)) {
          logInfo(s"Writelock for writing $blockId")
          return true
        } else {
          throw new RuntimeException(s"Cannot lock block $blockId")
        }
      }

      evictionPolicy.selectEvictFromDisagg(cost, blockId) {
        iter =>
          val discardSizeSum = iter.map(x => metricTracker.getBlockSize(x.blockId)).sum

          if (discardSizeSum < removalSize) {
            // just discard this block
            // because there is not enough space although we discard all of the blocks
            BlazeLogger.discardDisagg(blockId, cost.reduction,
              cost.disaggCost, estimateSize, "by master2")
            return false
          }

          val iterator = iter.toList.iterator
          val currTime = System.currentTimeMillis()
          var costSum = 0.0

          while (iterator.hasNext) {
            val discardBlock = iterator.next()
            val bid = discardBlock.blockId

            if (tryWriteLockHeldForDisagg(bid)) {
              disaggBlockInfo.get(bid) match {
                case None =>
                  // do nothing
                  releaseWriteLockForDisagg(bid)
                case Some(blockInfo) =>
                  if (blockInfo.writeDone) {
                    if (timeToRemove(blockInfo.createdTime, currTime)
                      && !recentlyRemoved.contains(bid) && totalDiscardSize < removalSize
                      && discardBlock.reduction <= cost.reduction
                      && costSum <= cost.reduction) {
                      costSum += discardBlock.reduction
                      totalDiscardSize += blockInfo.getActualBlockSize
                      removeBlocks.append((bid, blockInfo))
                      rmBlocks.append(discardBlock)
                    } else {
                      releaseWriteLockForDisagg(bid)
                    }
                  } else {
                    releaseWriteLockForDisagg(bid)
                  }
              }
            }
          }
      }
    }

    if (totalDiscardSize < removalSize) {
      // the cost due to discarding >  cost to store
      // we won't store it
      removeBlocks.foreach {
        pair => releaseWriteLockForDisagg(pair._1)
      }
      BlazeLogger.discardDisagg(blockId, cost.reduction, cost.disaggCost,
        estimateSize, "by master3")
      false
    } else {
      evictBlocks(removeBlocks.toList)
      rmBlocks.foreach { t =>
        BlazeLogger.evictDisagg(t.blockId, t.reduction, t.disaggCost,
          metricTracker.getBlockSize(t.blockId)) }
      BlazeLogger.logDisaggCaching(blockId, estimateBlockSize, cost.reduction, cost.disaggCost)
      addToDisagg(blockId, estimateBlockSize, cost)

      // We create info here and lock here
      val blockInfo = new CrailBlockInfo(blockId, executorId, getPath(blockId))
      disaggBlockInfo.put(blockId, blockInfo)
      // We should unlock it after file is created

      if (blockWriteLock(blockId, executorId)) {
        logInfo(s"Writelock for writing $blockId")
        true
      } else {
        throw new RuntimeException(s"Cannot get writelock $blockId 22")
      }
    }
  }

  private def evictBlocks(
         removeBlocks: List[(BlockId, CrailBlockInfo)]): Unit = {
    removeBlocks.foreach { b =>
      recentlyRemoved.put(b._1, true)

      executor.submit(new Runnable {
        override def run(): Unit = {
          removeFromDisagg(b._1)
          releaseWriteLockForDisagg(b._1)
        }
      })
    }
  }

  private def fileRead(blockId: BlockId, executorId: String): Int = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      0
    } else {
      val v = info.get
      logInfo(s"file read disagg block $blockId at $executorId")
      BlazeLogger.readDisagg(blockId)
      1
    }
  }

  private def removeRddsFromLocal(rdds: Set[Int]): Unit = {
    metricTracker.getExecutorBlocksMap.synchronized {
      metricTracker.getExecutorBlocksMap.foreach {
        entrySet =>
          val executorId = entrySet._1
          val blockSet = entrySet._2
          val removeSet = new mutable.HashSet[(BlockId, String)]()
          localExecutorLockMap(executorId).synchronized {
            blockSet.foreach {
              bid =>
                if (rdds.contains(bid.asRDDId.get.rddId)) {
                  removeSet.add((bid, executorId))
                }
                removeSet.foreach(pair => {
                  BlazeLogger.removeZeroBlocks(pair._1, pair._2)
                  removeFromLocal(pair._1, pair._2)
                })
            }
          }
      }
    }
  }

  private def removeDupRDDsFromDisagg: Unit = {
    disaggBlockInfo.keys.foreach(bid => {
      if (metricTracker.storedBlockInLocal(bid)) {
        if (tryWriteLockHeldForDisagg(bid)) {
          if (removeFromDisagg(bid).isDefined) {
            BlazeLogger.removeDuplicateBlocksInDisagg(bid)
          }
          releaseWriteLockForDisagg(bid)
        }
      }
    })
  }

  private def removeRddsFromDisagg(rdds: Set[Int]): Unit = {
    disaggBlockInfo.filter(pair => rdds.contains(pair._1.asRDDId.get.rddId))
      .keys.foreach(bid => {

      if (tryWriteLockHeldForDisagg(bid)) {
        BlazeLogger.removeZeroBlocksInDisagg(bid)
        removeFromDisagg(bid)
        releaseWriteLockForDisagg(bid)
      } else {
        logInfo(s"Failure of lock block for removing $bid")
      }
    })
  }

  private def timeToRemove(blockCreatedTime: Long, currTime: Long): Boolean = {
    currTime - blockCreatedTime > 4 * 1000
  }

  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  private def removeFromDisagg(blockId: BlockId): Option[CrailBlockInfo] = {
    logInfo(s"Disagg endpoint: file removed: $blockId")

    disaggBlockInfo.get(blockId) match {
      case None =>
        logWarning(s"Block is already removed !! $blockId")
        recentlyRemoved.remove(blockId)
        Option.empty
      case Some(info) =>
        if (info.writeDone) {
          val path = getPath(blockId)
          fs.delete(path, false).get()
          metricTracker.removeDisaggBlock(blockId)
          disaggBlockInfo.remove(blockId)
          recentlyRemoved.remove(blockId)
          Some(info)
        } else {
          logWarning(s"Block $blockId is being written")
          Option.empty
        }
    }
  }

  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  private def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      logWarning(s"No disagg block for writing $blockId")
      throw new RuntimeException(s"no disagg block for writing $blockId")
    } else {
      val v = info.get
      v.setSize(size)
      v.createdTime = System.currentTimeMillis()
      v.writeDone = true
      metricTracker.adjustDisaggBlockSize(blockId, v.getActualBlockSize)
      logInfo(s"Storing file writing $blockId, size $size")
      true
    }
  }

  def contains(blockId: BlockId): Int = {
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
       // logInfo(s"disagg not containing $blockId")
      0
    } else {
      1
    }
  }

  // For failure handling
  private val executorReadLockCount =
    new ConcurrentHashMap[String, ConcurrentHashMap[BlockId, AtomicInteger]].asScala
  private val executorWriteLockCount = new ConcurrentHashMap[String, mutable.Set[BlockId]].asScala

  private def blockWriteLock(blockId: BlockId, executorId: String): Boolean = {
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    if (disaggBlockLockMap(blockId).writeLock().tryLock()) {
      logInfo(s"Hold writelock $blockId, $executorId")
      executorWriteLockCount.putIfAbsent(executorId, new mutable.HashSet[BlockId])
      executorWriteLockCount(executorId).add(blockId)
      true
    } else {
      false
    }
  }

  private def blockWriteUnlock(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"Release writelock $blockId, $executorId")
    executorWriteLockCount(executorId).remove(blockId)
    disaggBlockLockMap(blockId).writeLock().unlock()
  }

  private def tryWriteLockHeldForDisagg(blockId: BlockId): Boolean = {
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    if (disaggBlockLockMap(blockId).writeLock().tryLock()) {
      logInfo(s"Hold tryWritelock $blockId")
      true
    } else {
      false
    }
  }

  private def releaseWriteLockForDisagg(blockId: BlockId): Unit = {
    logInfo(s"Release tryWritelock $blockId")
    disaggBlockLockMap(blockId).writeLock().unlock()
  }

  private def blockReadLock(blockId: BlockId, executorId: String): Boolean = {
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    if (disaggBlockLockMap(blockId).readLock().tryLock()) {
      logInfo(s"Hold readlock $blockId, $executorId")
      executorReadLockCount.putIfAbsent(executorId,
        new ConcurrentHashMap[BlockId, AtomicInteger]())
      executorReadLockCount(executorId).putIfAbsent(blockId, new AtomicInteger(0))
      executorReadLockCount(executorId).get(blockId).incrementAndGet()
      true
    } else {
      false
    }
  }

  private def blockReadUnlock(blockId: BlockId, executorId: String): Boolean = {
    logInfo(s"Release readlock $blockId, $executorId")
    val result = executorReadLockCount(executorId).get(blockId).decrementAndGet()
    disaggBlockLockMap(blockId).readLock().unlock()
    result >= 0
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // FOR DISAGG
    // FOR DISAGG
    case FileWriteEnd(blockId, executorId, size) =>
      // We unlock here because the lock is already hold by FileCreated
      if (!executorWriteLockCount.contains(executorId)
      || !executorWriteLockCount(executorId).contains(blockId)) {
        throw new RuntimeException(s"File write end should be called " +
          s"after holding lock $blockId, $executorId")
      }

      fileWriteEnd(blockId, size)
      blockWriteUnlock(blockId, executorId)

    case FileRemoved(blockId, executorId, remove) =>
      if (blockId.isRDD) {
        // logInfo(s"File removed call $blockId, $executorId")
        if (tryWriteLockHeldForDisagg(blockId)) {
          val info = removeFromDisagg(blockId)
          if (info.isDefined) {
            val size = info.get.getSize
            BlazeLogger.discardDisagg(blockId, 0, 0, size, "by local")
            context.reply(true)
          } else {
            context.reply(false)
          }
          releaseWriteLockForDisagg(blockId)
        } else {
          context.reply(false)
        }
      } else {
        context.reply(false)
      }

    case FileRead(blockId, executorId) =>
      // lock
      // logInfo(s"File read $blockId")
      if (blockReadLock(blockId, executorId)) {

        val result = fileRead(blockId, executorId)

        if (result == 0) {
          // logInfo(s"File unlock $blockId at $executorId for empty")
          blockReadUnlock(blockId, executorId)
        }

        context.reply(result)
      } else {
        // try again
        context.reply(2)
      }

    case FileReadUnlock(blockId, executorId) =>
      // logInfo(s"File read unlock $blockId from $executorId")
      // unlock
      // We unlock here because the lock is already hold by FileRead
      if (!executorReadLockCount.contains(executorId)) {
         throw new RuntimeException(s"FileReadUnlock 11 end should be called " +
          s"after holding lock $blockId, $executorId")
      }

      if (!blockReadUnlock(blockId, executorId)) {
        throw new RuntimeException(s"FileReadUnlock 22 end should be called " +
          s"after holding lock $blockId, $executorId")
      }

    case DiscardBlocksIfNecessary(estimateSize) =>
      throw new RuntimeException("not supported")

    case Contains(blockId, executorId) =>
      if (blockReadLock(blockId, executorId)) {
        try {
          val result = contains(blockId)
        } finally {
          blockReadUnlock(blockId, executorId)
        }
        context.reply(contains(blockId))
      } else {
        // try again
        context.reply(2)
      }

    case ReadDisaggBlock(blockId, time) =>
      BlazeLogger.deserTime(blockId, time)
      metricTracker.blockDeserCostMap.putIfAbsent(blockId, time)

    case WriteDisaggBlock(blockId, time) =>
      BlazeLogger.serTime(blockId, time)
      metricTracker.blockSerCostMap.putIfAbsent(blockId, time)

    case SendRecompTime(blockId, time) =>
      BlazeLogger.recompTime(blockId, time)

    case SendNoCachedRDDCompTime(rddId, time) =>
      BlazeLogger.rddCompTime(rddId, time)

    case GetSize(blockId, executorId) =>
      if (blockReadLock(blockId, executorId)) {
        try {
          val size = if (disaggBlockInfo.get(blockId).isEmpty) {
            logWarning(s"disagg block is empty.. no size $blockId")
            0L
          } else {
            disaggBlockInfo.get(blockId).get.getSize
          }
          context.reply(size)
        } finally {
          blockReadUnlock(blockId, executorId)
        }
      } else {
        context.reply(-1)
      }

    // FOR LOCAL
    // FOR LOCAL
    case StoreBlockOrNot(blockId, estimateSize, executorId, putDisagg, localFull) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)

      if (putDisagg) {
        context.reply(cachingDecision(blockId, estimateSize, executorId, putDisagg, localFull))
      } else {
        val lock = localExecutorLockMap(executorId)
        lock.synchronized {
          context.reply(cachingDecision(blockId, estimateSize, executorId, putDisagg, localFull))
        }
      }


    case CachingFail(blockId, estimateSize, executorId, putDisagg, localFull) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        cachingFail(blockId, estimateSize, executorId, putDisagg, localFull)
      }

    case ReadBlockFromLocal(blockId, executorId, fromRemote) =>
      BlazeLogger.readLocal(blockId, executorId, fromRemote)

    case IsRddCache(rddId) =>
      context.reply(isRddCache(rddId))

    case LocalEviction(blockId, executorId, size, prevEvicted) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        context.reply(localEviction(blockId, executorId, size + 1 * 1024 * 1024, prevEvicted))
      }
    case EvictionFail(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        localEvictionFail(blockId, executorId)
      }
    case LocalEvictionDone(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        localEvictionDone(blockId, executorId)
      }
    case CacheDisaggInMemory(blockId, size, executorId, enoughSpace) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        context.reply(cacheDisaggDataInMemory(blockId, size, executorId, enoughSpace))
      }

    case GetLocalBlockSize(blockId) =>
      if (metricTracker.localBlockSizeHistoryMap.containsKey(blockId)) {
        context.reply(metricTracker.localBlockSizeHistoryMap.get(blockId))
      } else {
        context.reply(-1L)
      }
  }
}




