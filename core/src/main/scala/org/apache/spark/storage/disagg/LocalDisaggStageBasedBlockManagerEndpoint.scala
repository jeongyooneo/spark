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
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint, RDDBlockId}
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
  extends DisaggBlockManagerEndpoint(false) {

  private val BLAZE_COST_FUNC =
    conf.get(BlazeParameters.COST_FUNCTION).contains("Blaze")

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  blockManagerMaster.setDisaggBlockManager(this)

  val autocaching = conf.get(BlazeParameters.AUTOCACHING)
  val executor: ExecutorService = Executors.newCachedThreadPool()
  val disableLocalCaching = conf.get(BlazeParameters.DISABLE_LOCAL_CACHING)
  private val USE_DISK = conf.get(BlazeParameters.USE_DISK)

  val recentlyBlockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()
  val localExecutorLockMap = new ConcurrentHashMap[String, Object]().asScala
  private val disaggBlockLockMap = new ConcurrentHashMap[BlockId, ReadWriteLock].asScala
  private val stageJobMap = new mutable.HashMap[Int, Int]()

  val scheduler = Executors.newSingleThreadScheduledExecutor()
  val task = new Runnable {
    def run(): Unit = {
      try {
        val costAnalyzerStart = System.currentTimeMillis()

        costAnalyzer.update
        /*
        val map = blockManagerMaster.blockManagerInfo.map {
          entry => val executor = entry._1.executorId
            val blocks = entry._2.blocks.filter(p => p._1.isRDD).keySet
            (executor, blocks)
        }.toMap

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
  scheduler.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)

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
  val recentlyEvictFailBlocksFromLocalDisk: mutable.Map[BlockId, Long] =
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

  private val rddCachedMap = new ConcurrentHashMap[Int, Boolean]()

  def isRddCache(rddId: Int): Boolean = {
    if (rddCachedMap.containsKey(rddId)) {
      rddCachedMap.get(rddId)
    } else {
      val cache = cachingPolicy.isRDDNodeCached(rddId)
      if (cache.nonEmpty) {
        rddCachedMap.putIfAbsent(rddId, cache.get)
        BlazeLogger.logCachingDecision(rddId)
        cache.get
      } else {
        false
      }
    }
  }

  def taskFinished(taskId: String): Unit = {
    logInfo(s"Handling task ${taskId} finished")
    metricTracker.taskFinished(taskId)
  }

  def taskStarted(taskId: String): Unit = {
    logInfo(s"Handling task ${taskId} started")
    metricTracker.taskStarted(taskId)
  }

  private var prevCleanupTime = System.currentTimeMillis()
  private val fullyProfiled = conf.get(BlazeParameters.FULLY_PROFILED)
  private val currJob = new AtomicInteger(-1)

  private val removedZeroRdds = new mutable.HashSet[Int]()

  def stageCompleted(stageId: Int): Unit = {
    logInfo(s"Handling stage ${stageId} completed in disagg manager")
    metricTracker.stageCompleted(stageId)

    if (autocaching) {
      // removeDupRDDsFromDisagg
      autocaching.synchronized {
        if (System.currentTimeMillis() - prevCleanupTime >= 10000) {
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
              removedZeroRdds.synchronized {
                removedZeroRdds.add(rdd)
              }
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

  private val stagePartitionMap = new ConcurrentHashMap[Int, Int]()
  private val rddDiscardMap = new ConcurrentHashMap[Int, ConcurrentHashMap[Int, Boolean]]()

  def stageSubmitted(stageId: Int, jobId: Int, partition: Int): Unit = synchronized {
    stagePartitionMap.put(stageId, partition)
    stageJobMap.put(stageId, jobId)
    logInfo(s"Stage submitted ${stageId}, jobId: $jobId, " +
      s"partition: ${partition}, jobMap: $stageJobMap")
    currJob.set(jobId)
    metricTracker.stageSubmitted(stageId)
  }

  def removeFromLocal(blockId: BlockId, executorId: String,
                      onDisk: Boolean): Unit = {
    metricTracker.removeExecutorBlock(blockId, executorId, onDisk)
  }

  // Private methods

  private def addToLocal(blockId: BlockId, executorId: String,
                         estimateSize: Long, onDisk: Boolean): Unit = {
    metricTracker.addExecutorBlock(blockId, executorId, estimateSize, onDisk)
  }

  private def cachingFail(blockId: BlockId, estimateSize: Long,
                              executorId: String,
                              putDisagg: Boolean, localFull: Boolean,
                          onDisk: Boolean): Unit = {
    BlazeLogger.cachingFailure(blockId, executorId, estimateSize, onDisk)
    removeFromLocal(blockId, executorId, onDisk)
  }

  private val previouslyEvicted = new ConcurrentHashMap[BlockId, Boolean]()

  private val zigZagRatio = conf.get(BlazeParameters.ZIGZAG_RATIO)

  private def cachingDecision(blockId: BlockId, estimateSize: Long,
                              executorId: String,
                              putDisagg: Boolean, localFull: Boolean,
                              onDisk: Boolean,
                              promote: Boolean): Boolean = {

    val cachingDecStart = System.currentTimeMillis

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
    val storingCost = costAnalyzer.compDisaggCost(executorId, blockId)

    if (estimateSize == 0) {
      logInfo(s"RDD estimation size zero $blockId")
    }

    if (!putDisagg) {

      if (!BLAZE_COST_FUNC) {
        // We just cache  if it is not blaze cost function !!
        // (if it is LRC or MRD)
        if (conf.get(BlazeParameters.COST_FUNCTION).contains("LCS")
          && onDisk
        && storingCost.compCost < storingCost.disaggCost) {
          BlazeLogger.discardLocal(blockId, executorId,
            storingCost.cost, storingCost.disaggCost, estimateSize, "LCS", onDisk)
          recentlyRecachedBlocks.remove(blockId)
          return false
        } else {
          addToLocal(blockId, executorId, estimateSize, onDisk)
          BlazeLogger.logLocalCaching(blockId, executorId,
            estimateSize, storingCost.cost, 0,
            "1", onDisk)
          return true
        }
      }

      if (disableLocalCaching) {
        // Do not cache blocks in local
        // just return false
        if (!onDisk) {
          BlazeLogger.discardLocal(blockId, executorId,
            storingCost.cost, storingCost.disaggCost, estimateSize, "onlyDisagg", onDisk)
          recentlyRecachedBlocks.remove(blockId)
          false
        } else {
          BlazeLogger.logLocalCaching(blockId, executorId,
            estimateSize, storingCost.cost, storingCost.disaggCost,
            "2", onDisk)

          if (recentlyRecachedBlocks.remove(blockId).isDefined) {
            BlazeLogger.recacheDisaggToLocal(blockId, executorId)
          }
          true
        }
      } else {


        if (localFull) {
          if (storingCost.cost <= 0 || promote) {
            // We do not PROMOTE if the local full because it will cause additional
            // evictions
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.disaggCost,
              estimateSize, s"$estimateSize, Local Full, cost 0 or Promote", onDisk)
            return false
          }

          /*
          if (zigZagRatio < 1 && previouslyEvicted.containsKey(blockId)) {
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.disaggCost,
              estimateSize, s"$estimateSize, PreviouslyEvicted", onDisk)
            return false
          }
          */

          val evictList = localEviction(
            Some(blockId), executorId, estimateSize, Set.empty, false, true)

          if (evictList.nonEmpty) {
            addToLocal(blockId, executorId, estimateSize, onDisk)
            BlazeLogger.logLocalCaching(blockId, executorId,
              estimateSize, storingCost.compCost, storingCost.disaggCost,
              "2", onDisk)

            if (recentlyRecachedBlocks.remove(blockId).isDefined) {
              BlazeLogger.recacheDisaggToLocal(blockId, executorId)
            }

            true
          } else {
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.disaggCost,
              estimateSize, s"$estimateSize, Local Full", onDisk)
            recentlyRecachedBlocks.remove(blockId)
            false
          }
        } else {
          // local caching
          // put until threshold
          if (storingCost.cost <= 0) {
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.disaggCost,
              estimateSize, s"$estimateSize", onDisk)
            return false
          }

          if (onDisk) {

            val rddId = blockId.asRDDId.get.rddId
            val s = blockId.toString.split("_")
            val blockIndex = s(2).toInt

            logInfo(s"RDD cost: ${blockId}, " +
              s"recomp: ${storingCost.compCost}, "
              + s"disk: ${storingCost.disaggCost}, " +
              s"${storingCost.compCost/storingCost.futureUse}, " +
              s"numShuffle: ${storingCost.numShuffle}," +
              s"storeInDisk: ${storingCost.onDisk}," +
              s"size: $estimateSize")

            if (estimateSize == 0) {
              addToLocal(blockId, executorId, estimateSize, onDisk)
              BlazeLogger.logLocalCaching(blockId, executorId,
                estimateSize, storingCost.compCost, storingCost.disaggCost, "3", onDisk)

              if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                BlazeLogger.recacheDisaggToLocal(blockId, executorId)
              }
              return true
            }

            if (storingCost.compCost == storingCost.disaggCost) {
              // This is the case that the data is already cached in disk
              BlazeLogger.discardLocal(blockId, executorId,
                storingCost.compCost, storingCost.disaggCost,
                estimateSize, s"$estimateSize", onDisk)
              return false
            } else if (storingCost.compCost < storingCost.disaggCost) {
              rddDiscardMap.putIfAbsent(rddId, new ConcurrentHashMap[Int, Boolean]())

              val discardSet = rddDiscardMap.get(rddId)
              val node = rddJobDag.get.getRDDNode(blockId)
              val (parentNodes, childNodes) = rddJobDag.get.getParentChildCachedNodes(node)
              val numPartition = stagePartitionMap.get(node.rootStage)

              /*
              val checkZigZag =
                (parentNodes ++ childNodes)
                .filter(node => rddDiscardMap.containsKey(node.rddId))
                .map(node => rddDiscardMap.get(node.rddId))
                .filter(map => map.containsKey(blockIndex)).isEmpty
              */

              discardSet.synchronized {
                // if (zigZagRatio >= 1 || (checkZigZag
                //  && discardSet.size() <= numPartition * zigZagRatio)) {
                  logInfo(s"Discard intermediate " +
                    s"by cost comparison: ${blockId}, ${storingCost.compCost}, "
                    + s"${storingCost.disaggCost}, " +
                    s"${storingCost.compCost / storingCost.futureUse}, " +
                    s"numShuffle: ${storingCost.numShuffle}, size: $estimateSize")

                  // previouslyEvicted.put(blockId, true)

                  discardSet.put(blockIndex, true)

                  BlazeLogger.discardLocal(blockId, executorId,
                    storingCost.compCost, storingCost.disaggCost,
                    estimateSize, s"$estimateSize", onDisk)
                  return false
                /*
                } else {

                  previouslyEvicted.remove(blockId)

                  addToLocal(blockId, executorId, estimateSize, onDisk)
                  BlazeLogger.logLocalCaching(blockId, executorId,
                    estimateSize, storingCost.compCost, storingCost.disaggCost, "5", onDisk)

                  if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                    BlazeLogger.recacheDisaggToLocal(blockId, executorId)
                  }
                  return true
                }
                */
              }
              // }
            } else {

              // previouslyEvicted.remove(blockId)

              val cachingDecElapsed = System.currentTimeMillis - cachingDecStart

              addToLocal(blockId, executorId, estimateSize, onDisk)
              BlazeLogger.logLocalCaching(blockId, executorId,
                estimateSize, storingCost.compCost, storingCost.disaggCost, "3", onDisk)

              if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                BlazeLogger.recacheDisaggToLocal(blockId, executorId)
              }

              true
            }
          } else {

              /*
            if (zigZagRatio < 1 && previouslyEvicted.containsKey(blockId)) {
              BlazeLogger.discardLocal(blockId, executorId,
                storingCost.compCost, storingCost.disaggCost,
                estimateSize, s"$estimateSize, PreviouslyEvicted", onDisk)
              return false
            }
            */

            addToLocal(blockId, executorId, estimateSize, onDisk)
            BlazeLogger.logLocalCaching(blockId, executorId,
              estimateSize, storingCost.compCost, storingCost.disaggCost, "3", onDisk)

            if (recentlyRecachedBlocks.remove(blockId).isDefined) {
              BlazeLogger.recacheDisaggToLocal(blockId, executorId)
            }

            true

          }
        }
      }
    } else {
      throw new RuntimeException("Disagg decision is disabled!")
      // disaggDecision(blockId, estimateSize, executorId, true)
    }
  }

  private def localEviction(blockId: Option[BlockId],
                            executorId: String, evictionSize: Long,
                            prevEvicted: Set[BlockId],
                            onDisk: Boolean,
                            cachingDecision: Boolean): List[BlockId] = {

    val evictionList: mutable.ListBuffer[BlockId] = new ListBuffer[BlockId]
    val blockManagerId = blockManagerMaster.executorBlockManagerMap.get(executorId).get
    val blockManagerInfo = blockManagerMaster.blockManagerInfo(blockManagerId)

    val currTime = System.currentTimeMillis()
    var sizeSum = 0L

    var evictSize = evictionSize

    evictionPolicy.selectEvictFromLocal(
      new CompDisaggCost(RDDBlockId(0, -1), Double.MaxValue), executorId, evictSize, onDisk) {
      iter =>
        /*
          if (onDisk) {
            iter
              .filter(discardingBlock => blockManagerInfo.blocks.contains(discardingBlock.blockId)
                && blockManagerInfo.blocks(discardingBlock.blockId).diskSize > 0)
              .map(b => b.blockId)
          } else {
            iter
              .filter(discardingBlock => blockManagerInfo.blocks.contains(discardingBlock.blockId)
                && blockManagerInfo.blocks(discardingBlock.blockId).memSize > 0)
              .map(b => b.blockId)
          }
          */

        if (iter.isEmpty) {
          logWarning(s"Low comp disagg block is empty " +
            s"for evicting $blockId, size $evictSize")
          List.empty
        } else if (blockId.isEmpty || !blockId.get.isRDD || !BLAZE_COST_FUNC) {
          // Spilling
          return iter.map(m => m.blockId)
              .filter(bid => metricTracker.localMemStoredBlocksMap.get(executorId).contains(bid))

          /*
          val map = if (onDisk) {
            recentlyEvictFailBlocksFromLocalDisk
          } else {
            recentlyEvictFailBlocksFromLocal
          }

          iter.foreach {
            discardingBlock => {
              if (!prevEvicted.contains(discardingBlock.blockId)
                && discardingBlock.blockId != blockId.get) {
                val elapsed = currTime - map.getOrElse(discardingBlock.blockId, 0L)
                val createdTime = metricTracker
                  .recentlyBlockCreatedTimeMap.get(discardingBlock.blockId)
                if (elapsed > 5000 && timeToRemove(createdTime, System.currentTimeMillis())) {
                  map.remove(discardingBlock.blockId)
                  if (blockManagerInfo.blocks.contains(discardingBlock.blockId)) {
                    if (onDisk &&
                      blockManagerInfo.blocks(discardingBlock.blockId).diskSize > 0) {
                      sizeSum += blockManagerInfo.blocks(discardingBlock.blockId).diskSize
                      evictionList.append(discardingBlock.blockId)
                    } else if (!onDisk &&
                      blockManagerInfo.blocks(discardingBlock.blockId).memSize > 0) {
                      sizeSum += blockManagerInfo.blocks(discardingBlock.blockId).memSize
                      evictionList.append(discardingBlock.blockId)
                    }
                  }
                }
              }
            }
          }

          if (sizeSum >= evictSize) {
            return evictionList.toList
          } else {
            logWarning(s"Size sum for no blaze $sizeSum < eviction Size $evictSize, " +
              s"for caching ${blockId} selected list: $evictionList")
            List.empty
          }
          */
        } else if (cachingDecision) {

          var sum = 0.0

          val map = if (onDisk) {
            recentlyEvictFailBlocksFromLocalDisk
          } else {
            recentlyEvictFailBlocksFromLocal
          }

          val storingCost = costAnalyzer.compDisaggCost(executorId, blockId.get)

          iter.filter(m => metricTracker
            .localMemStoredBlocksMap
            .get(executorId).contains(m.blockId)).foreach {
            discardingBlock => {
              if (!prevEvicted.contains(discardingBlock.blockId)
                && discardingBlock.cost <= storingCost.cost
                && discardingBlock.blockId != blockId.get) {
                val elapsed = currTime - map.getOrElse(discardingBlock.blockId, 0L)
                val createdTime = metricTracker
                  .recentlyBlockCreatedTimeMap.get(discardingBlock.blockId)
                if (elapsed > 5000 && timeToRemove(createdTime, System.currentTimeMillis())) {
                  map.remove(discardingBlock.blockId)
                  if (blockManagerInfo.blocks.contains(discardingBlock.blockId)) {
                    if (onDisk &&
                      blockManagerInfo.blocks(discardingBlock.blockId).diskSize > 0 &&
                      sum <= storingCost.cost) {
                      sum += discardingBlock.cost
                      sizeSum += blockManagerInfo.blocks(discardingBlock.blockId).diskSize
                      evictionList.append(discardingBlock.blockId)
                    } else if (!onDisk &&
                      blockManagerInfo.blocks(discardingBlock.blockId).memSize > 0 &&
                      sum <= storingCost.cost) {
                      sum += discardingBlock.cost
                      sizeSum += blockManagerInfo.blocks(discardingBlock.blockId).memSize
                      evictionList.append(discardingBlock.blockId)
                    }
                  }
                }
              }
            }

              if (sizeSum >= evictSize + 5 * 1024 * 1024) {
                logInfo(s"CostSum: $sum, block: $blockId")
                return evictionList.toList
              }
          }

          if (sizeSum >= evictSize) {
            logInfo(s"CostSum: $sum, block: $blockId")
            return evictionList.toList
          } else {
            logWarning(s"Size sum $sizeSum < eviction Size $evictSize, " +
              s"for caching ${blockId} selected list: $evictionList")
            List.empty
          }
        } else {
          return iter.map(m => m.blockId)
            .filter(bid => metricTracker.localMemStoredBlocksMap.get(executorId).contains(bid))
        }
    }
  }

  private def localEvictionFail(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    logInfo(s"Local eviction failed from disk $onDisk: $blockId, $executorId")
    if (!onDisk) {
      recentlyEvictFailBlocksFromLocal.put(blockId, System.currentTimeMillis())
    } else {
      recentlyEvictFailBlocksFromLocalDisk.put(blockId, System.currentTimeMillis())
    }
    // addToLocal(blockId, executorId, metricTracker.localBlockSizeHistoryMap.get(blockId))
  }

  private def localEvictionDone(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    val cost = costAnalyzer.compDisaggCost(executorId, blockId)
    BlazeLogger.evictLocal(
      blockId, executorId, cost.compCost, cost.disaggCost,
      metricTracker.getBlockSize(blockId), onDisk)
    removeFromLocal(blockId, executorId, onDisk)
  }


  private val recentlyRecachedBlocks = new ConcurrentHashMap[BlockId, Boolean]().asScala

  def promoteToMemory(blockId: BlockId, size: Long,
                      executorId: String,
                      enoughSpace: Boolean): Boolean = {

    // We only recache the block if the block was stored in the executor
    if (disableLocalCaching || metricTracker.storedBlockInLocalMemory(blockId) ) {
      return false
    }

    if (!BLAZE_COST_FUNC) {
      // Promotion in LRC and MRD
      return true
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

    val costForStoredBlock = costAnalyzer.compDisaggCost(executorId, blockId)

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

    val cost = costAnalyzer.compDisaggCost(executorId, blockId)
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

    if (cost.cost <= 0 || USE_DISK) {
      BlazeLogger.discardDisagg(
        blockId, cost.cost, cost.disaggCost, estimateSize, "by master0")
      return false
    }

    disaggBlockLockMap.synchronized {
      if (disaggBlockInfo.contains(blockId)) {
        BlazeLogger.discardDisagg(
          blockId, cost.compCost, cost.disaggCost, estimateSize, "by master1")
        return false
      }


      // If we have enough space in disagg memory, cache it
      if (metricTracker.disaggTotalSize.get() + estimateBlockSize < disaggThreshold) {
        BlazeLogger.logDisaggCaching(blockId, executorId, estimateBlockSize,
          cost.compCost, cost.disaggCost)
        addToDisagg(blockId, estimateBlockSize, cost)

        // We create info here and lock here
        val blockInfo = new CrailBlockInfo(blockId, getPath(blockId))
        disaggBlockInfo.put(blockId, blockInfo)
        // We should unlock it after file is created
        if (blockWriteLock(blockId, executorId)) {
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
            BlazeLogger.discardDisagg(blockId, cost.compCost,
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
                    if (discardBlock.cost <= 0) {
                      totalDiscardSize += blockInfo.getActualBlockSize
                      removeBlocks.append((bid, blockInfo))
                      rmBlocks.append(discardBlock)
                    } else if (timeToRemove(blockInfo.createdTime, currTime)
                      && !recentlyRemoved.contains(bid) && totalDiscardSize < removalSize
                      && discardBlock.cost <= cost.cost
                      && costSum <= cost.cost) {
                      costSum += discardBlock.cost
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
      BlazeLogger.discardDisagg(blockId, cost.compCost, cost.disaggCost,
        estimateSize, "by master3")
      false
    } else {
      evictBlocks(removeBlocks.toList)
      rmBlocks.foreach { t =>
        BlazeLogger.evictDisagg(t.blockId, t.compCost, t.disaggCost,
          metricTracker.getBlockSize(t.blockId)) }
      BlazeLogger.logDisaggCaching(blockId, executorId,
        estimateBlockSize, cost.compCost, cost.disaggCost)
      addToDisagg(blockId, estimateBlockSize, cost)

      // We create info here and lock here
      val blockInfo = new CrailBlockInfo(blockId, getPath(blockId))
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

  private def fileCreated(blockId: BlockId): Boolean = {
    disaggBlockInfo.get(blockId) match {
      case None =>
        logInfo(s"Disagg endpoint: file created: $blockId")
        val blockInfo = new CrailBlockInfo(blockId, getPath(blockId))
        if (disaggBlockInfo.putIfAbsent(blockId, blockInfo).isEmpty) {
          true
        } else {
          false
        }
      case Some(v) =>
        false
    }
  }

  private def fileRead(blockId: BlockId, executorId: String): Int = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      0
    } else {
      val v = info.get
      logInfo(s"file read disagg block $blockId at $executorId")
      BlazeLogger.readDisagg(blockId, executorId)
      1
    }
  }

  override def removeRddsFromLocal(rdds: Set[Int]): Unit = {
    metricTracker.getExecutorLocalMemoryBlocksMap.synchronized {
      metricTracker.getExecutorLocalMemoryBlocksMap.foreach {
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
                  removeFromLocal(pair._1, pair._2, false)
                })
            }
          }
      }
    }

    metricTracker.getExecutorLocalDiskBlocksMap.synchronized {
      metricTracker.getExecutorLocalDiskBlocksMap.foreach {
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
                  BlazeLogger.removeZeroBlocksDisk(pair._1, pair._2)
                  removeFromLocal(pair._1, pair._2, true)
                })
            }
          }
      }
    }
  }

  /*
  private def removeDupRDDsFromDisagg: Unit = {
    disaggBlockInfo.keys.foreach(bid => {
      if (metricTracker.storedBlockInLocalMemory(bid)) {
        if (tryWriteLockHeldForDisagg(bid)) {
          if (removeFromDisagg(bid).isDefined) {
            BlazeLogger.removeDuplicateBlocksInDisagg(bid)
          }
          releaseWriteLockForDisagg(bid)
        }
      }
    })
  }
  */

  private def removeRddsFromDisagg(rdds: Set[Int]): Unit = {
    disaggBlockInfo.filter(pair => rdds.contains(pair._1.asRDDId.get.rddId))
      .keys.foreach(bid => {

      if (tryWriteLockHeldForDisagg(bid)) {
        BlazeLogger.removeZeroBlocksInDisagg(bid)
        removeFromDisagg(bid)
        releaseWriteLockForDisagg(bid)
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
      throw new RuntimeException("Disagg read disabled")
          /*
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
      */

    case FileReadUnlock(blockId, executorId) =>
      throw new RuntimeException("Disagg read unlock disabled")

      /*

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
      */

    case DiscardBlocksIfNecessary(estimateSize) =>
      throw new RuntimeException("not supported")

    case Contains(blockId, executorId) =>
      throw new RuntimeException("Disagg contain disabled")

      /*
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
      */

    case ReadDisaggBlock(blockId, size, time) =>
      BlazeLogger.deserTime(blockId, size, time)
      metricTracker.blockDeserCostMap.putIfAbsent(blockId, time)

    case WriteDisaggBlock(blockId, size, time) =>
      BlazeLogger.serTime(blockId, size, time)
      metricTracker.blockSerCostMap.putIfAbsent(blockId, time)

    case SendRecompTime(blockId, time) =>
      BlazeLogger.recompTime(blockId, time)

    case SendNoCachedRDDCompTime(rddId, time) =>
      BlazeLogger.rddCompTime(rddId, time)

    case GetSize(blockId, executorId) =>
      throw new RuntimeException("Disagg getStream disabled")

      /*
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
      */

    // FOR LOCAL
    // FOR LOCAL
    case StoreBlockOrNot(blockId, estimateSize, executorId, putDisagg,
    localFull, onDisk, promote) =>
      val start = System.currentTimeMillis()
      logInfo(s"Start cachingDecision ${blockId}, " +
        s"executor ${executorId}, ${putDisagg}, ${localFull}, ${onDisk}, ${promote}")
      if (putDisagg) {
        synchronized {
          localExecutorLockMap.putIfAbsent(executorId, new Object)
          context.reply(cachingDecision(blockId, estimateSize, executorId,
            putDisagg, localFull, onDisk, promote))
        }
      } else {
        localExecutorLockMap.putIfAbsent(executorId, new Object)
        context.reply(cachingDecision(blockId, estimateSize, executorId,
          putDisagg, localFull, onDisk, promote))
      }
      val end = System.currentTimeMillis()
      logInfo(s"End cachingDecision ${blockId}, time ${end - start} ms," +
        s"executor ${executorId}, ${putDisagg}, ${localFull}, ${onDisk}")

    case CachingDone(blockId, size, executorId, onDisk) =>
      val rddId = blockId.asRDDId.get.rddId
      rddDiscardMap.putIfAbsent(rddId, new ConcurrentHashMap[Int, Boolean]())
      val set = rddDiscardMap.get(rddId)
      set.synchronized {
        val s = blockId.toString.split("_")
        val blockIndex = s(2).toInt
        if (set.contains(blockIndex)) {
          set.remove(blockIndex)
        }
      }
      BlazeLogger.cachingDone(blockId, executorId, size, onDisk)

    case DiskCachingDone(blockId, size, executorId) =>
      BlazeLogger.cachingDone(blockId, executorId, size, true)
      metricTracker.localDiskStoredBlocksSizeMap.put(blockId, size)

    case CachingFail(blockId, estimateSize, executorId, putDisagg, localFull, onDisk) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        cachingFail(blockId, estimateSize, executorId, putDisagg, localFull, onDisk)
      }

    case ReadBlockFromLocal(blockId, executorId, fromRemote, onDisk, rtime) =>
      BlazeLogger.readLocal(blockId, executorId, fromRemote, onDisk, rtime)

    case IsRddCache(rddId) =>
      context.reply(isRddCache(rddId))

    case LocalEviction(blockId, executorId, size, prevEvicted, onDisk) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        context.reply(localEviction(blockId, executorId,
          size, prevEvicted, onDisk, false))
      }
    case EvictionFail(blockId, executorId, onDisk) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        localEvictionFail(blockId, executorId, onDisk)
      }
    case LocalEvictionDone(blockId, executorId, onDisk) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        localEvictionDone(blockId, executorId, onDisk)
      }
    case PromoteToMemory(blockId, size, executorId, enoughSpace, fromDisk) =>
      BlazeLogger.tryToPromote(blockId, executorId, size, fromDisk)
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        context.reply(promoteToMemory(blockId, size, executorId, enoughSpace))
      }
    case GetLocalBlockSize(blockId) =>
      if (metricTracker.localBlockSizeHistoryMap.containsKey(blockId)) {
        context.reply(metricTracker.localBlockSizeHistoryMap.get(blockId))
      } else {
        context.reply(-1L)
      }

    case SendRDDElapsedTime(srcBlock, dstBlock, clazz, time) =>
      val key = s"${srcBlock}-${dstBlock}"
      logInfo(s"Block elapsed time ${key}: ${time}, class: ${clazz}")
      metricTracker.blockElapsedTimeMap.put(key, time)
  }

}





