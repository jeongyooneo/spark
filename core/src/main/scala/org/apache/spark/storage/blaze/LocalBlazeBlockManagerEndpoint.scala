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

package org.apache.spark.storage.blaze

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReadWriteLock, StampedLock}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.storage.blaze.BlazeBlockManagerMessages._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint, RDDBlockId}
import org.apache.spark.util.ThreadUtils

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}
import scala.concurrent.{Await, ExecutionContext, duration}
import scala.concurrent.duration.Duration

/**
 */
private[spark] class LocalBlazeBlockManagerEndpoint(override val rpcEnv: RpcEnv,
                                                    val isLocal: Boolean,
                                                    conf: SparkConf,
                                                    blockManagerMaster: BlockManagerMasterEndpoint,
                                                    clusterDiskCapacityMB: Long,
                                                    val costAnalyzer: CostAnalyzer,
                                                    val metricTracker: MetricTracker,
                                                    val cachingPolicy: CachingPolicy,
                                                    val evictionPolicy: EvictionPolicy,
                                                    val rddJobDag: Option[RDDJobDag])
  extends BlazeBlockManagerEndpoint {

  private val BLAZE_COST_FUNC =
    conf.get(BlazeParameters.COST_FUNCTION).contains("Blaze")

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  blockManagerMaster.setBlazeBlockManager(this)

  val autounpersist = conf.get(BlazeParameters.AUTOUNPERSIST)
  val executor: ExecutorService = Executors.newCachedThreadPool()
  val disableLocalCaching = conf.get(BlazeParameters.DISABLE_LOCAL_CACHING)

  val recentlyBlockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()
  val localExecutorLockMap = new ConcurrentHashMap[String, Object]().asScala
  private val blockIdToLockMap = new ConcurrentHashMap[BlockId, ReadWriteLock].asScala

  /*
  val scheduler = Executors.newSingleThreadScheduledExecutor()
  val task = new Runnable {
    def run(): Unit = {
      try {
        costAnalyzer.update
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
*/

  // For Blaze
  val diskThreshold: Long = clusterDiskCapacityMB * (1024 * 1024)
  val blockIdToBlazeMetadataMap: concurrent.Map[BlockId, BlazeBlockMetadata] =
    new ConcurrentHashMap[BlockId, BlazeBlockMetadata]().asScala
  val recentlyRemoved: mutable.Map[BlockId, Boolean] =
    new mutable.HashMap[BlockId, Boolean]()

  /**
   * Parameters for local caching.
   */
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
          bid => blockIdToLockMap(bid).writeLock().unlock()
            if (!blockIdToBlazeMetadataMap(bid).writeDone) {
              blockIdToBlazeMetadataMap.remove(bid)
            }
        }
      }

      if (executorReadLockCount.contains(executorId)) {
        val blockCounterMap = executorReadLockCount.remove(executorId).get.asScala
        blockCounterMap.foreach {
          entry => val bid = entry._1
            val cnt = entry._2
            for (i <- 1 to cnt.get()) {
              blockIdToLockMap(bid).readLock().unlock()
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
        if (cache.get) {
          BlazeLogger.logCachingDecision(rddId)
        }
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
  private val removedZeroRdds = new mutable.HashSet[Int]()

  private val currJob = metricTracker.currJob

  def stageCompleted(stageId: Int): Unit = {
    logInfo(s"Handling stage ${stageId} completed in BlazeManager")


    if (autounpersist) {
      autounpersist.synchronized {
        if (!metricTracker.completedStages.contains(stageId)) {
          metricTracker.stageCompleted(stageId)
          // unpersist rdds
          val zeroRDDs = costAnalyzer.findZeroCostRDDs
            .filter {
              p =>
                val rddNode = rddJobDag.get.getRDDNode(p)
                val lastStage = rddNode.getStages.max

                val incompleteStage = rddJobDag.get.dag(rddNode).map { n => n.rootStage }
                  .filter( s => !metricTracker.completedStages.contains(s) )

                logInfo(s"Zero RDD edges ${rddJobDag.get.dag(rddNode)}, " +
                  s"completedStage ${metricTracker.completedStages}," +
                  s"incomplete for rdd ${incompleteStage}")

                incompleteStage.isEmpty
                /*
                logInfo(s"StateCompleted ${stageId} LastJob of RDD " +
                  s"${rddNode.rddId}: stage $lastStage, " +
                  s"currJob: ${currJob.get()}, jobMap: ${metricTracker.stageJobMap}")

                if (metricTracker.stageJobMap.contains(lastStage)) {
                  logInfo(s"StateCompleted ${stageId} LastJob of RDD " +
                    s"${rddNode.rddId}: stage $lastStage, " +
                    s"jobId: ${metricTracker.stageJobMap(lastStage)}, " +
                    s"currJob: ${currJob.get()}, jobMap: ${metricTracker.stageJobMap}")

                  true
                } else {
                  true
                }
                */
            }

          zeroRDDs.foreach {
            rdd =>
              // val rddNode = rddJobDag.get.getRDDNode(rdd)
              // logInfo(s"Zero RDD edges ${rddJobDag.get.dag(rddNode)}, " +
              //  s"completedStage ${metricTracker.completedStages}")

              val remove = true
              /*
              val repeatedNode = rddJobDag.get
                .findRepeatedNode(rddNode, rddNode, new mutable.HashSet[RDDNode]())
              val remove = repeatedNode match {
                case Some(rnode) =>
                  val crossJobRef = rddJobDag.get.numCrossJobReference(rnode)
                  logInfo(s"TG try to remove RDD ${rdd},  rddjob ${rddNode.jobId}, " +
                    s"currjob: ${currJob} " +
                    s"repeated RDD ${rnode.rddId} job ${rnode.jobId}, crossRef: ${crossJobRef}")
                  if (rddNode.jobId == currJob.get() && crossJobRef > 0) {
                    // Do not remove this RDD node
                    false
                  } else {
                    true
                  }
                case None =>
                  logInfo(s"TG try to remove RDD ${rdd},  rddjob ${rddNode.jobId}, " +
                    s"currjob: ${currJob}" +
                    s"No repeated RDD")
                  true
              }
              */

              if (remove) {
                logInfo(s"Remove zero cost rdd $rdd from memory...")
                // remove from local executors
                removedZeroRdds.synchronized {
                  removedZeroRdds.add(rdd)
                }
                // scalastyle:off awaitresult
                Await.result(blockManagerMaster.removeRdd(rdd), Duration.create(10,
                  duration.SECONDS))
              }

              // scalastyle:on awaitresult
          }

          removeRddsFromLocal(zeroRDDs)
          // removeRddsFromDisagg(zeroRDDs)

          prevCleanupTime = System.currentTimeMillis()
        }
      }
    } else {
      metricTracker.stageCompleted(stageId)
    }

  }

  private val stagePartitionMap = new ConcurrentHashMap[Int, Int]()
  private val rddDiscardMap = new ConcurrentHashMap[Int, ConcurrentHashMap[Int, Boolean]]()

  def jobSubmitted(jobId: Int): Unit = {
    currJob.set(jobId)
  }

  def stageSubmitted(stageId: Int, jobId: Int, partition: Int): Unit = synchronized {
      stagePartitionMap.put(stageId, partition)
      metricTracker.stageJobMap.put(stageId, jobId)
      logInfo(s"Stage submitted ${stageId}, jobId: $jobId, " +
        s"partition: ${partition}, jobMap: ${metricTracker.stageJobMap}")
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
                          executorId: String, onDisk: Boolean): Unit = {
    BlazeLogger.cachingFailure(blockId, executorId, estimateSize, onDisk)
    removeFromLocal(blockId, executorId, onDisk)
  }

  private val alwaysCache = conf.get(BlazeParameters.ALWAYS_CACHE)

  private def cachingDecision(blockId: BlockId, estimateSize: Long,
                              executorId: String,
                              localFull: Boolean,
                              onDisk: Boolean,
                              promote: Boolean,
                              taskAttempt: Long): Boolean = {
    blockIdToLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())

    val t = System.currentTimeMillis()
    metricTracker.blockCreatedTimeMap.putIfAbsent(blockId, t)
    metricTracker.localStoredBlocksHistoryMap
      .putIfAbsent(executorId, ConcurrentHashMap.newKeySet[BlockId].asScala)
    metricTracker.localStoredBlocksHistoryMap.get(executorId).add(blockId)
    metricTracker.recentlyBlockCreatedTimeMap.put(blockId, t)
    val storingCost = costAnalyzer.compCostWithTaskAttempt(executorId, blockId, taskAttempt)

      if (!BLAZE_COST_FUNC || alwaysCache) {
        // We just cache  if it is not blaze cost function !!
        // (if it is LRC or MRD)
        if (!(BLAZE_COST_FUNC && onDisk)) {
          if (conf.get(BlazeParameters.COST_FUNCTION).contains("LCS")
            && onDisk
            && storingCost.compCost < storingCost.diskCost) {
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.cost, storingCost.diskCost, estimateSize, "LCS", onDisk)
            recentlyRecachedBlocks.remove(blockId)
            return false
          } else {
            return true
          }
        }
      }


      if (estimateSize < 0) {
        logInfo(s"RDD estimation size zero $blockId")
        addToLocal(blockId, executorId, estimateSize, onDisk)
        BlazeLogger.logLocalCaching(blockId, executorId,
          estimateSize, storingCost.cost, 0,
          "FirstAndStore", onDisk)
        throw new RuntimeException(s"hahaha estimate size ${estimateSize} for block ${blockId}")
      }

      if (disableLocalCaching) {
        // Do not cache blocks in local
        // just return false
        if (!onDisk) {
          BlazeLogger.discardLocal(blockId, executorId,
            storingCost.cost, storingCost.diskCost, estimateSize, "onlyDisagg", onDisk)
          recentlyRecachedBlocks.remove(blockId)
          false
        } else {
          BlazeLogger.logLocalCaching(blockId, executorId,
            estimateSize, storingCost.cost, storingCost.diskCost,
            "2", onDisk)

          if (recentlyRecachedBlocks.remove(blockId).isDefined) {
            BlazeLogger.promote(blockId, executorId)
          }
          true
        }
      } else {

        if (localFull) {
          if (storingCost.cost <= 0 || promote) {
            // We do not PROMOTE if the local full because it will cause additional
            // evictions
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.diskCost,
              estimateSize, s"$estimateSize, Local Full, cost 0 or Promote", onDisk)
            return false
          }

          val evictList = localEviction(
            Some(blockId), executorId, estimateSize, Set.empty, false, true)

          if (evictList.nonEmpty) {
            addToLocal(blockId, executorId, estimateSize, onDisk)
            BlazeLogger.logLocalCaching(blockId, executorId,
              estimateSize, storingCost.compCost, storingCost.diskCost,
              "2", onDisk)

            if (recentlyRecachedBlocks.remove(blockId).isDefined) {
              BlazeLogger.promote(blockId, executorId)
            }

            true
          } else {
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.diskCost,
              estimateSize, s"$estimateSize, Local Full", onDisk)
            recentlyRecachedBlocks.remove(blockId)
            false
          }
        } else {
          // local caching
          // put until threshold
          if (storingCost.cost <= 0) {
            BlazeLogger.discardLocal(blockId, executorId,
              storingCost.compCost, storingCost.diskCost,
              estimateSize, s"$estimateSize", onDisk)
            return false
          }

          if (onDisk) {

            val rddId = blockId.asRDDId.get.rddId
            val s = blockId.toString.split("_")
            val blockIndex = s(2).toInt

            logInfo(s"RDD cost: ${blockId}, " +
              s"recomp: ${storingCost.compCost}, "
              + s"disk: ${storingCost.diskCost}, " +
              s"${storingCost.compCost/storingCost.futureUse}, " +
              s"numShuffle: ${storingCost.numShuffle}," +
              s"storeInDisk: ${storingCost.onDisk}," +
              s"size: $estimateSize")

            if (estimateSize == 0) {
              addToLocal(blockId, executorId, estimateSize, onDisk)
              BlazeLogger.logLocalCaching(blockId, executorId,
                estimateSize, storingCost.compCost, storingCost.diskCost, "3", onDisk)

              if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                BlazeLogger.promote(blockId, executorId)
              }
              return true
            }

            if (storingCost.compCost == storingCost.diskCost) {
              // This is the case that the data is already cached in disk
              BlazeLogger.discardLocal(blockId, executorId,
                storingCost.compCost, storingCost.diskCost,
                estimateSize, s"$estimateSize", onDisk)
              return false
            } else if (storingCost.compCost < storingCost.diskCost) {
              rddDiscardMap.putIfAbsent(rddId, new ConcurrentHashMap[Int, Boolean]())

              val discardSet = rddDiscardMap.get(rddId)
              val node = rddJobDag.get.getRDDNode(blockId)

              discardSet.synchronized {
                logInfo(s"Discard intermediate " +
                    s"by cost comparison: ${blockId}, ${storingCost.compCost}, "
                    + s"${storingCost.diskCost}, " +
                    s"${storingCost.compCost / storingCost.futureUse}, " +
                    s"numShuffle: ${storingCost.numShuffle}, size: $estimateSize")

                  // previouslyEvicted.put(blockId, true)

                  discardSet.put(blockIndex, true)

                  BlazeLogger.discardLocal(blockId, executorId,
                    storingCost.compCost, storingCost.diskCost,
                    estimateSize, s"$estimateSize", onDisk)
                  return false
              }
            } else {
              addToLocal(blockId, executorId, estimateSize, onDisk)
              BlazeLogger.logLocalCaching(blockId, executorId,
                estimateSize, storingCost.compCost, storingCost.diskCost, "3", onDisk)

              if (recentlyRecachedBlocks.remove(blockId).isDefined) {
                BlazeLogger.promote(blockId, executorId)
              }

              true
            }
          } else {
            addToLocal(blockId, executorId, estimateSize, onDisk)
            BlazeLogger.logLocalCaching(blockId, executorId,
              estimateSize, storingCost.compCost, storingCost.diskCost, "3", onDisk)

            if (recentlyRecachedBlocks.remove(blockId).isDefined) {
              BlazeLogger.promote(blockId, executorId)
            }

            true
          }
        }
      }
  }

  private def localEviction(blockId: Option[BlockId],
                            executorId: String, sizeToEvict: Long,
                            prevEvicted: Set[BlockId],
                            onDisk: Boolean,
                            cachingDecision: Boolean): List[BlockId] = {

    val evictionList: mutable.ListBuffer[BlockId] = new ListBuffer[BlockId]
    val blockManagerId = blockManagerMaster.executorBlockManagerMap.get(executorId).get
    val blockManagerInfo = blockManagerMaster.blockManagerInfo(blockManagerId)
    var sizeSum = 0L

    evictionPolicy.selectLocalEvictionCandidate(
      new CompCost(RDDBlockId(0, -1), Double.MaxValue), executorId, sizeToEvict, onDisk) {
      iter =>
        if (iter.isEmpty) {
          List.empty
        } else if (blockId.isEmpty || !blockId.get.isRDD
          || !BLAZE_COST_FUNC || alwaysCache ||
          !metricTracker.sizePredictionMap.containsKey(blockId.get)) {
          // Spilling
          return iter.toList.map(m => m.blockId)
              .filter(bid => metricTracker.localMemStoredBlocksMap.get(executorId).contains(bid))
        } else if (cachingDecision) {
          var sum = 0.0
          val map = if (onDisk) {
            recentlyEvictFailBlocksFromLocalDisk
          } else {
            recentlyEvictFailBlocksFromLocal
          }

          iter.synchronized {
            val storingCost = costAnalyzer.compCost(executorId, blockId.get)

            val tobeEvictList =
              new mutable.HashMap[Int, mutable.ListBuffer[(BlockId, CompCost)]]()

            for (i <- 0 until iter.length) {
              var discardingBlock = iter(i)

              if (metricTracker.localMemStoredBlocksMap
                .get(executorId).contains(discardingBlock.blockId)) {

                // If block needs to be updated, we sort it again
                // UPDATE BLOCK COST !!!!!!!
                // UPDATE BLOCK COST !!!!!!!
                val discardingIndex = discardingBlock.blockId.name.split("_")(2).toInt
                if (tobeEvictList.contains(discardingIndex)) {
                  val evictDependentPartitions = tobeEvictList(discardingIndex)

                  if (discardingBlock.updatedBlocks.isEmpty) {
                    discardingBlock.updatedBlocks = Some(new mutable.HashSet[BlockId]())
                  }

                  if (!discardingBlock.updatedBlocks.get.equals(
                    evictDependentPartitions.map(a => a._1).toSet)) {

                    logInfo(s"Updating ${discardingBlock.blockId} for ${evictDependentPartitions}")
                    evictDependentPartitions.foreach {
                      entry =>
                        val alreadyEvict = entry._1
                        val alreadyEvictCost = entry._2

                        if (!discardingBlock.updatedBlocks.get.contains(alreadyEvict)) {
                          discardingBlock.updatedBlocks.get.add(alreadyEvict)

                          if (alreadyEvict.asRDDId.get.rddId <
                            discardingBlock.blockId.asRDDId.get.rddId) {
                            // this is parent
                            // we should add the recomp/or read time
                            if (alreadyEvictCost.compCost < alreadyEvictCost.diskCost) {
                              // This will be discarded, so we add the time
                              discardingBlock.addRecomp(alreadyEvictCost.recompTime)
                            }
                          } else {
                            // this is child
                            // we increase future use
                            if (alreadyEvictCost.compCost < alreadyEvictCost.diskCost) {
                              discardingBlock.increaseFutureUse(alreadyEvictCost.futureUse.toInt)
                            }
                          }
                        }
                    }
                  }
                }
                // UPDATE BLOCK COST END !!!!!!!
                // UPDATE BLOCK COST END !!!!!!!

                if (discardingBlock.updated) {
                  if (i < iter.length - 1 && discardingBlock.cost > iter(i + 1).cost) {
                    // just discard
                    // bubble-sorting
                    val temp = iter(i + 1)
                    iter(i) = temp
                    discardingBlock = temp
                    iter(i + 1) = discardingBlock
                  }
                }

                if (!prevEvicted.contains(discardingBlock.blockId)
                  && discardingBlock.cost <= storingCost.cost
                  && discardingBlock.blockId != blockId.get) {

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

                if (sizeSum >= sizeToEvict + 5 * 1024 * 1024) {
                  logInfo(s"CostSum: $sum, block: $blockId, $evictionList")
                  return evictionList.toList
                }
              }
            }

            if (sizeSum >= sizeToEvict) {
              logInfo(s"CostSum: $sum, block: $blockId")
              return evictionList.toList
            } else {
              logWarning(s"Size sum $sizeSum < eviction Size $sizeToEvict, " +
                s"for caching ${blockId} selected list: $evictionList")
              List.empty
            }
          }
        } else {
          return iter.toList.map(m => m.blockId)
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
  }

  private def localEvictionDone(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    val cost = costAnalyzer.compCost(executorId, blockId)
    BlazeLogger.evictLocal(
      blockId, executorId, cost.compCost, cost.diskCost,
      metricTracker.getBlockSize(blockId), onDisk)
    removeFromLocal(blockId, executorId, onDisk)
  }


  private val recentlyRecachedBlocks = new ConcurrentHashMap[BlockId, Boolean]().asScala

  private val disablePromote = conf.get(BlazeParameters.DISABLE_PROMOTE)

  def promoteToMemory(blockId: BlockId, size: Long,
                      executorId: String,
                      enoughSpace: Boolean): Boolean = {

    if (!BLAZE_COST_FUNC || alwaysCache) {
      // Promotion in LRC and MRD
      return true
    }

    // We only recache the block if the block was stored in the executor
    if (disableLocalCaching || metricTracker.storedBlockInLocalMemory(blockId) ) {
      return false
    }

    // Code smell..
    if (disablePromote) {
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

    val costForStoredBlock = costAnalyzer.compCost(executorId, blockId)

    if (evictionPolicy.promote(costForStoredBlock, executorId, blockId, size)
      && recentlyRecachedBlocks.putIfAbsent(blockId, true).isEmpty) {
      // we store this rdd and evict others
      true
    } else {
      false
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
                logInfo(s"RDDs: ${rdds}, bid: ${bid}")
                if (rdds.contains(bid.asRDDId.get.rddId)) {
                  removeSet.add((bid, executorId))
                }

            }
            removeSet.foreach(pair => {
              BlazeLogger.removeZeroBlocks(pair._1, pair._2)
              removeFromLocal(pair._1, pair._2, false)
            })
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

  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  private def removeFromDisagg(blockId: BlockId): Option[BlazeBlockMetadata] = {
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
   */

  // For failure handling
  private val executorReadLockCount =
    new ConcurrentHashMap[String, ConcurrentHashMap[BlockId, AtomicInteger]].asScala
  private val executorWriteLockCount = new ConcurrentHashMap[String, mutable.Set[BlockId]].asScala

  private def blockWriteLock(blockId: BlockId, executorId: String): Boolean = {
    blockIdToLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    if (blockIdToLockMap(blockId).writeLock().tryLock()) {
      logInfo(s"Hold writelock $blockId, $executorId")
      executorWriteLockCount.putIfAbsent(executorId, new mutable.HashSet[BlockId])
      executorWriteLockCount(executorId).add(blockId)
      true
    } else {
      false
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SendSerTime(blockId, size, time) =>
      BlazeLogger.serTime(blockId, size, time)
      metricTracker.blockSerCostMap.putIfAbsent(blockId, time)

    case SendRecompTime(blockId, time) =>
      BlazeLogger.recompTime(blockId, time)

    case SendCompTime(rddId, time) =>
      BlazeLogger.rddCompTime(rddId, time)

    // FOR LOCAL
    case StoreBlockOrNot(blockId, estimateSize, executorId,
    localFull, onDisk, promote, attempt) =>
      val start = System.currentTimeMillis()
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      context.reply(cachingDecision(blockId, estimateSize, executorId,
        localFull, onDisk, promote, attempt))
      val end = System.currentTimeMillis()
      logInfo(s"cachingDecision for ${blockId} took time ${end - start} ms," +
        s"executor ${executorId}, ${localFull}, ${onDisk}")

    case CachingDone(blockId, size, stageId, onDisk) =>
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
      BlazeLogger.cachingDone(blockId, stageId, size, onDisk)

    case DiskCachingDone(blockId, size, executorId) =>
      BlazeLogger.cachingDone(blockId, executorId, size, true)
      metricTracker.localDiskStoredBlocksSizeMap.put(blockId, size)

    case CachingFail(blockId, estimateSize, executorId, onDisk) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        cachingFail(blockId, estimateSize, executorId, onDisk)
      }

    case ReadBlockFromLocal(blockId, stageId, fromRemote, onDisk, rtime) =>
      BlazeLogger.readLocal(blockId, stageId, fromRemote, onDisk, rtime)

    case IsRddToCache(rddId) =>
      context.reply(isRddCache(rddId))

    case SendSize(blockId, executorId, size) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)

      if (blockId.isRDD) {
        metricTracker.localBlockSizeHistoryMap.putIfAbsent(blockId, size)
        val cost = costAnalyzer.compCost(executorId, blockId)
        addToLocal(blockId, executorId, size, false)
        BlazeLogger.logLocalCaching(blockId, executorId,
          size, cost.compCost, cost.diskCost, "FirstStoring", false)
      }

    case SizePrediction(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)

      if (blockId.isRDD) {
        metricTracker.localStoredBlocksHistoryMap
          .putIfAbsent(executorId, ConcurrentHashMap.newKeySet[BlockId].asScala)
        metricTracker.blockCreatedTimeMap.putIfAbsent(blockId, System.currentTimeMillis())
        metricTracker.localStoredBlocksHistoryMap.get(executorId).add(blockId)
        metricTracker.recentlyBlockCreatedTimeMap.put(blockId, System.currentTimeMillis())

        rddJobDag match {
          case Some(dag) =>
            if (metricTracker.sizePredictionMap.containsKey(blockId)) {
              context.reply(metricTracker.sizePredictionMap.get(blockId))
            }

            val rddNode = dag.getRDDNode(blockId)
            val index = blockId.name.split("_")(2).toInt
            val callsiteNode =
              dag.findSameCallsiteParent(rddNode, rddNode, new mutable.HashSet[RDDNode](),
                index, blockId)

            logInfo(s"size prediction for ${blockId} with ${callsiteNode}")

            if (callsiteNode.isDefined) {
              val callsiteBlock = RDDBlockId(callsiteNode.get.rddId, index)
              val size = metricTracker.localBlockSizeHistoryMap.get(callsiteBlock)
              metricTracker.sizePredictionMap.putIfAbsent(blockId, size)
              context.reply(size)
            } else {
              context.reply(-1L)
            }

          case None => context.reply(-1L)
        }
      } else {
        context.reply(-1L)
      }

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

    case TaskAttempBlockId(taskAttemp, blockId) =>
      val key = s"taskAttemp-${blockId.name}"
      metricTracker.taskAttempBlockCount.putIfAbsent(key, new AtomicInteger())
      metricTracker.taskAttempBlockCount.get(key).incrementAndGet()

    case SendRDDElapsedTime(srcBlock, dstBlock, clazz, time) =>
      val key = s"${srcBlock}-${dstBlock}"
      logInfo(s"Block elapsed time ${key}: ${time}, class: ${clazz}")
      if (metricTracker.blockElapsedTimeMap.containsKey(key)) {
        metricTracker.blockElapsedTimeMap.put(key,
          Math.max(metricTracker.blockElapsedTimeMap.get(key), time))
      } else {
        metricTracker.blockElapsedTimeMap.put(key, time)
      }
  }

}





