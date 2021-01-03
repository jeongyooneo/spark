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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.disagg.RDDJobDag.StageDistance
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.mortbay.util.ajax.JSON

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}
import scala.io.Source

class RDDJobDag(val dag: mutable.Map[RDDNode, mutable.Set[RDDNode]],
                val reverseDag: mutable.Map[RDDNode, mutable.Set[RDDNode]],
                val metricTracker: MetricTracker) extends Logging {

  val dagChanged = new AtomicBoolean(false)
  private var  prevVertices: Map[Int, RDDNode] = dag.keySet.map(node => (node.rddId, node)).toMap
  val explicitCachingRDDs = new mutable.HashSet[Int]()

  def vertices: Map[Int, RDDNode] = {
    val start = System.currentTimeMillis()
    if (dagChanged.get()) {
      dagChanged.set(false)
    }
    val elapsed = System.currentTimeMillis() - start
    // logInfo(s"RDDJobDAG vertices(dagChanged=true) took $elapsed ms")
    prevVertices
  }

  private val updatedStage = new mutable.HashSet[Int]()

  def onlineUpdate(stageId: Int,
                   newDag: Map[RDDNode, mutable.Set[RDDNode]]): Unit = synchronized {

    if (updatedStage.contains(stageId)) {
      return
    }

    updatedStage.add(stageId)

    logInfo(s"Online update stage for ${stageId}")
    val newDagNodes = newDag.keys
      .filter(newNode => newNode.rootStage == stageId)

    // find RDD node different from existing nodes
    val diffNodes = newDagNodes
      .filter(newNode => {
      if (vertices.contains(newNode.rddId)) {
        val originNode = getRDDNode(newNode.rddId)
        val result = !originNode.callsite.equals(newNode.callsite)
        if (result) {
          logInfo(s"New Node ${newNode.rddId} different from existing node," +
            s" new callsite: ${newNode.callsite}, existing callsite: ${originNode.callsite}")
        }
        result
      } else {
        true
      }
    })

    val newDagNodeIds = newDagNodes.map { n => n.rddId }.toSet

    logInfo(s"New DAG Nodes for stage ${stageId}: ${newDagNodeIds}")

    if (diffNodes.nonEmpty) {
      logInfo(s"Detected different nodes ${diffNodes}")

      // We should rebuild the DAG
      // 1) Remove the stages > stageId and nodes
      val minStage = diffNodes.map(n => n.rootStage).min

      logInfo(s"Min stage for diff nodes ${minStage}")

      val removableNodes = dag.keys.filter(node => node.rootStage >= minStage)
      logInfo(s"Removable nodes ${removableNodes.map { n => n.rddId }}")

      // Remove nodes
      removableNodes.foreach {
        removableNode =>
          logInfo(s"Remove existing node ${removableNode.rddId} stage ${removableNode.rootStage}")
          dag.remove(removableNode)
          reverseDag.remove(removableNode)
      }

      // Add the nodes
      newDagNodes.foreach {
        newNode =>
          if (!dag.contains(newNode)) {
            dag(newNode) = new mutable.HashSet[RDDNode]()
            reverseDag(newNode) = new mutable.HashSet[RDDNode]()
          } else {
            dag.keys.filter(p => p.rddId == newNode.rddId).foreach {
              p => p.addRefStage(stageId)
            }
          }
      }

      // Add edges
      val newReverse = RDDJobDag.buildReverseDag(newDag)
      newDagNodes.foreach {
        newNode => val edges = newDag(newNode)
          edges.foreach {
            child => dag(newNode).add(child)
              reverseDag(child).add(newNode)
              logInfo(s"Add new edge ${newNode.rddId}->${child.rddId} stage ${newNode.rootStage}")
          }
        val reverseEdges = newReverse(newNode)
          reverseEdges.foreach {
            parent =>
              dag(parent).add(newNode)
              reverseDag(newNode).add(parent)
              logInfo(s"Add new reverse edge" +
                s" ${parent.rddId}->${newNode.rddId} stage ${newNode.rootStage}")
          }
      }
    }

    /*
    // update dag
    newDag.foreach { pair =>
      val parent = pair._1
      pair._2.foreach {
        child =>
          if (!reverseDag.contains(child)) {
            reverseDag(child) = new mutable.HashSet[RDDNode]()
          } else {
            reverseDag.keys.filter(p => p.rddId == child.rddId).foreach {
              p => p.addRefStages(child.getStages)
            }
          }
          reverseDag(child).add(parent)
      }

      if (!dag.contains(pair._1)) {
        logInfo(s"New vertex is created ${pair._1}->${pair._2}")
        dag.put(pair._1, pair._2)
        prevVertices = dag.keySet.map(node => (node.rddId, node)).toMap
        dagChanged.set(true)
      } else {

        dag.keys.filter(p => p.rddId == pair._1.rddId).foreach {
          p => p.addRefStages(pair._1.getStages)
        }

        // TODO: if autocaching false, we should set cached here

        if (!pair._2.subsetOf(dag(pair._1))) {
          pair._2.foreach { dag(pair._1).add(_) }
          logInfo(s"New edges are created for RDD " +
            s"${pair._1.rddId}, ${dag(pair._1)}")
          prevVertices = dag.keySet.map(node => (node.rddId, node)).toMap
          dagChanged.set(true)
        }
      }
    }

      // compare with the reverse dag dependency
      // to find DAG mistmatch !!
      // if there exist additional dependencies not matched with the submitted job
      // we should fix the dag
      val newReverseDag = RDDJobDag.buildReverseDag(newDag)
      val unmatchedRDDs = newReverseDag.filter {
        pair =>
          val node = pair._1
          val newReverseDagParents = pair._2
          val mismatch = !newReverseDagParents.equals(reverseDag(node))
          if (mismatch) {
            logInfo(s"Mismatched RDD ${node}, " +
              s"additional parents ${reverseDag(node).diff(newReverseDagParents)}")
          }
          mismatch
      }.toList

      if (unmatchedRDDs.nonEmpty) {
        unmatchedRDDs.foreach {
          pair =>
            val child = pair._1
            val parents = pair._2
            val prevParents = reverseDag(child)
            val diff = prevParents.diff(parents)
            diff.foreach {
              prevParent =>
                reverseDag(child).remove(prevParent)
                dag(prevParent).remove(child)
            }
        }
        prevVertices = dag.keySet.map(node => (node.rddId, node)).toMap
        dagChanged.set(true)
      }
      */

      logInfo(s"DAG after update: $dag")
      logInfo(s"ReverseDAG after update: $reverseDag")
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("--------------------------------------------\n")
    for ((key, v) <- dag) {
      sb.append(s"[$key -> $v]\n")
    }
    sb.append("--------------------------------------------\n")
    sb.toString()
  }

  private def blockIdToRDDId(blockId: BlockId): Int = {
    blockId.asRDDId.get.rddId
  }

  private def getBlockId(rddId: Int, childBlockId: BlockId): BlockId = {
    val index = childBlockId.name.split("_")(2).toInt
    RDDBlockId(rddId, index)
  }

  private def getTaskId(stageId: Int, blockId: BlockId): String = {
    val index = blockId.name.split("_")(2).toInt
    s"$stageId-$index-0"
  }

  private def findRootStageStartTimes(rddNode: RDDNode, blockId: BlockId):
  (ListBuffer[BlockId], ListBuffer[Long]) = {
    findRootStageStartTimeHelper(rddNode, blockId, new mutable.HashSet[RDDNode]())
  }

  private def findRootStageStartTimeHelper(rddNode: RDDNode, blockId: BlockId,
                                           visited: mutable.Set[RDDNode]):
  (ListBuffer[BlockId], ListBuffer[Long]) = {
    val b: ListBuffer[BlockId] = mutable.ListBuffer[BlockId]()
    val l: ListBuffer[Long] = mutable.ListBuffer[Long]()

    if (visited.contains(rddNode)) {
      return (b, l)
    }

    visited.add(rddNode)

    if (!reverseDag.contains(rddNode) || reverseDag(rddNode).isEmpty) {
      // This is root, so we get the task start time of the root stage
      val rootTaskId = getTaskId(rddNode.rootStage, blockId)
      metricTracker.taskStartTime.get(rootTaskId) match {
        case None =>
          throw new RuntimeException(s"Task start time is none" +
            s" for ${blockId}, taskid: ${rootTaskId}")
          /*
          metricTracker.getTaskStartTimeFromBlockId(rddNode.rootStage, blockId) match {
            case None =>
              b.append(blockId)
              l.append(metricTracker.stageStartTime(rddNode.rootStage))
            case Some(startTime) =>
              b.append(blockId)
              l.append(startTime)
          }
         */
        case Some(startTime) =>
          b.append(blockId)
          l.append(startTime)
      }
    } else {
      for (parent <- reverseDag(rddNode)) {
        // logInfo(s"recursive findRootStageStartTimes for $blockId, " +
        //  s"current: $rddNode, parent: $parent")
        val (bb, ll) = findRootStageStartTimeHelper(parent, blockId, visited)
        b.appendAll(bb)
        l.appendAll(ll)
      }
    }

    // logInfo(s"findRootStageStartTimes for $blockId took $elapsed ms")

    (b, l)
  }

  private def findCachedCallsiteUnrollingTime(rddNode: RDDNode,
                                 index: Int,
                                 childBlockId: BlockId,
                                 cacheMap: mutable.Map[BlockId, (Long, Option[RDDNode])]):
  (Long, Option[RDDNode]) = {

    if (cacheMap.contains(childBlockId)) {
      return cacheMap(childBlockId)
    }

    val index = childBlockId.name.split("_")(2).toInt
    val callsite = findSameCallsiteParent(
      rddNode, rddNode, new mutable.HashSet[RDDNode](), index, childBlockId)
    if (callsite.isDefined) {
      val callsiteUnrollingKey = s"unroll-eviction-rdd_${callsite.get.rddId}_$index"
      val callsiteBlock = RDDBlockId(callsite.get.rddId, index)
      if (metricTracker.blockElapsedTimeMap.containsKey(callsiteUnrollingKey)) {
        val result = (metricTracker.blockElapsedTimeMap.get(callsiteUnrollingKey), callsite)
        cacheMap.put(callsiteBlock, result)
        return result
      } else {
        return findCachedCallsiteUnrollingTime(
          callsite.get, index, callsiteBlock, cacheMap)
      }
    }
    (0L, None)
  }

  private def dfsGetBlockElapsedTime(myRDD: Int,
                                     childBlockId: BlockId,
                                     nodeCreatedTime: Long,
                                     visited: mutable.Set[RDDNode],
                                     map: mutable.Map[BlockId, (Long, Option[RDDNode])]):
  (ListBuffer[String], ListBuffer[Long], Int) = {

    val b: ListBuffer[String] = mutable.ListBuffer[String]()
    val l: ListBuffer[Long] = mutable.ListBuffer[Long]()
    var numShuffle = 0

    val rddId = blockIdToRDDId(childBlockId)
    val rddNode = vertices(rddId)

    if (visited.contains(rddNode)) {
      return (b, l, numShuffle)
    }

    visited.add(rddNode)

    // Materialized time
    val unrollingKey = s"unroll-eviction-${childBlockId.name}"
    var timeSum = metricTracker.blockElapsedTimeMap.getOrDefault(unrollingKey, 0L)

    // For cached node
    if (timeSum == 0 && dag(rddNode).size >= 2) {
      val index = childBlockId.name.split("_")(2).toInt
      val result = findCachedCallsiteUnrollingTime(rddNode, index, childBlockId, map)
      timeSum += result._1
      logDebug(s"Callsite finding for unrolling ${childBlockId}: ${result._2}, time: ${result._1}")

    }

    // val evictionKey = s"eviction-${childBlockId.name}"
    // var evictionTime = metricTracker.blockElapsedTimeMap.getOrDefault(evictionKey, 0L)

    b.append(unrollingKey)
    l.append(timeSum)

    // b.append(evictionKey)
    // l.append(evictionTime)

    if (reverseDag.contains(rddNode) && reverseDag(rddNode).nonEmpty) {
      for (parent <- reverseDag(rddNode)) {
        val parentBlockId = getBlockId(parent.rddId, childBlockId)
        val key = s"${parentBlockId.name}-${childBlockId.name}"
        val elapsedTimeFromParent = metricTracker.blockElapsedTimeMap.get(key)
        val added = elapsedTimeFromParent

        b.append(key)

        if (metricTracker.blockStored(parentBlockId)) {
          // logInfo(s"RDD ${myRDD} DFS from " +
          //  s"${childBlockId} to ${parentBlockId}: stored")
          if (metricTracker.localDiskStoredBlocksMap.containsKey(parentBlockId)) {
            // If it is cached, we should read it from mem or disk
            // If it is stored in disk, we should add disk read overhead
            val parentUnrollKey = s"unroll-${parentBlockId.name}"
            val parentEvictionKey = s"eviction-${parentBlockId.name}"
            var diskoverhead = BlazeParameters.readThp * metricTracker.getBlockSize(parentBlockId)

            diskoverhead += metricTracker.blockElapsedTimeMap.getOrDefault(parentUnrollKey, 0L)
            l.append(added + diskoverhead.toLong)
          } else {
            l.append(added)
          }
        } else if (parent.shuffled) {
          val parentUnrollKey = s"unroll-${parentBlockId.name}"
          l.append(added + metricTracker.blockElapsedTimeMap.getOrDefault(parentUnrollKey, 0L))
          numShuffle += 1
        } else {
          l.append(added)

          val (bb, ll, s) =
            dfsGetBlockElapsedTime(myRDD, parentBlockId,
              nodeCreatedTime, visited, map)
          // logInfo(s"RDD ${myRDD} DFS from " +
          //  s"${childBlockId} to ${parentBlockId}: ${bb}, ${ll}, shuffle: $s")

          b.appendAll(bb)
          l.appendAll(ll)

          numShuffle += s
        }
      }
    }

    (b, l, numShuffle)
  }

  def blockCompTime(blockId: BlockId, nodeCreatedTime: Long): (Long, Int) = {
    val rddId = blockIdToRDDId(blockId)
    val (parentBlocks, times, numShuffle) =
      dfsGetBlockElapsedTime(rddId, blockId,
        nodeCreatedTime, new mutable.HashSet[RDDNode](),
        new mutable.HashMap[BlockId, (Long, Option[RDDNode])]())
     logDebug(s"BlockComptTime of ${blockId}:" +
       s" ${parentBlocks}, " + s"${times}, shuffle: $numShuffle")

    // disk overhead for shuffle
    val size = metricTracker.getBlockSize(blockId)
    ((times.sum + BlazeParameters.readThp * size * numShuffle).toLong, numShuffle)
      /*
    if (numShuffle > 3) {
      t + 10000000
    } else {
      // logInfo(s"BlockComptTime of ${blockId}: ${parentBlocks}, " + s"${times}")
      t
    }
    */
  }

  def containsRDD(rddId: Int): Boolean = {
    vertices.contains(rddId)
  }

  def getRefCntRDD(rddId: Int): Int = {
    val rddNode = vertices(rddId)
    val edges = dag(rddNode)

    val repeat = findRepeatedNode(rddNode, rddNode, new mutable.HashSet[RDDNode]())
    repeat match {
      case Some(rnode) =>
        // consider repeated pattern
        if (edges.size != dag(rnode).size) {
          dag(rnode).size
        } else {
          edges.size
        }
      case None =>
        edges.size
    }

    // logInfo(s"Edges for rdd $rddId ${edges}")
    // edges.map(p => p.rootStage).size
  }

  def getUnpersistCnt(blockId: BlockId): Int = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    val refStage = rddNode.getStages.filter(s => !metricTracker.completedStages.contains(s))
    var cnt = refStage.size

    try {
      for (childnode <- dag(rddNode)) {
        if (!metricTracker.completedStages.contains(childnode.rootStage)
        && !refStage.contains(childnode.rootStage)) {
          cnt += 1
        }
      }
      cnt
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logWarning(s"Exception happend !! for finding rdd node ${rddNode.rddId}")
        throw new RuntimeException(e)
    }
  }

  def getLRCRefCnt(blockId: BlockId): Int = {

    var cnt = 0
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    try {
      for (childnode <- dag(rddNode)) {
        if (!metricTracker.completedStages.contains(childnode.rootStage)) {
          cnt += 1
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logWarning(s"Exception happend !! for finding rdd node ${rddNode.rddId}")
    }

    cnt
  }

  def getMRDStage(blockId: BlockId): Int = {
    var cnt = 0
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    val list = new ListBuffer[Int]

    try {
      for (childnode <- dag(rddNode)) {
        if (!metricTracker.completedStages.contains(childnode.rootStage)) {
          list.append(childnode.rootStage)
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logWarning(s"Exception happend !! for finding rdd node ${rddNode.rddId}")
    }

    if (list.isEmpty) {
      0
    } else {
      list.min
    }
  }

  private def collectMRDBlocks(rddNode: RDDNode, blockId: BlockId,
                                         stageSet: mutable.HashSet[Int],
                                         visitedRdds: mutable.HashSet[Int],
                                         distance: Int,
                                         absoluteDistance: Int,
                                         prevcached: Int): Map[Int, StageDistance] = {

    val map = new mutable.HashMap[Int, StageDistance]

    if (visitedRdds.contains(rddNode.rddId)) {
      return map.toMap
    } else {
      visitedRdds.add(rddNode.rddId)
    }

    try {
      for (childnode <- dag(rddNode)) {
        val childBlockId = getBlockId(childnode.rddId, blockId)
        var newDist = distance
        var prevc = prevcached
        var absolute = absoluteDistance
        absolute += 1

        if (map.contains(childnode.rootStage)) {
          // update
          if (map(childnode.rootStage).distance < distance) {
            map(childnode.rootStage).distance = distance
            map(childnode.rootStage).absolute = absolute
            map(childnode.rootStage).prevCached = prevc
          }
        }

        if (dag(childnode).size >= 2) {
          prevc += 1
        }

        if (!stageSet.contains(childnode.rootStage) &&
          !metricTracker.completedStages.contains(childnode.rootStage)) {
          stageSet.add(childnode.rootStage)
          newDist += 1
          map.put(childnode.rootStage,
            new StageDistance(childnode.rootStage, newDist, absolute, prevc))
        }

        // if (getRefCntRDD(childnode.rddId) < 2) {
        collectMRDBlocks(childnode, blockId,
          stageSet, visitedRdds, newDist, absolute, prevc).foreach {
          entry => map.put(entry._1, entry._2)
        }
        // }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logWarning(s"Exception happend !! for finding rdd node ${rddNode.rddId}")
    }

    map.toMap
  }

  def getRDDNode(rddId: Int): RDDNode = {
    vertices(rddId)
  }

  private val cachedNodesMap = new ConcurrentHashMap[RDDNode, (Set[RDDNode], Set[RDDNode])]()

  def getParentChildCachedNodes(rddNode: RDDNode): (Set[RDDNode], Set[RDDNode]) = {
    if (cachedNodesMap.containsKey(rddNode)) {
      cachedNodesMap.get(rddNode)
    } else {
      cachedNodesMap.putIfAbsent(rddNode,
        (findCachedParentNodes(rddNode), findCachedChildNodes(rddNode)))
      cachedNodesMap.get(rddNode)
    }
  }
  private def findCachedChildNodes(rddNode: RDDNode): Set[RDDNode] = {
    var set = new mutable.HashSet[RDDNode]()

    if (dag.contains(rddNode) && dag(rddNode).nonEmpty) {
      for (child <- dag(rddNode)) {
        if (dag(child).size >= 2) {
          set.add(child)
        } else {
          set = set.union(findCachedChildNodes(child))
        }
      }
    }

    set.toSet
  }

  private def findCachedParentNodes(rddNode: RDDNode): Set[RDDNode] = {
    var set = new mutable.HashSet[RDDNode]()

    if (reverseDag.contains(rddNode) && reverseDag(rddNode).nonEmpty) {
      for (parent <- reverseDag(rddNode)) {
        if (dag(parent).size >= 2) {
          set.add(parent)
        } else {
          set = set.union(findCachedParentNodes(parent))
        }
      }
    }

    set.toSet
  }

  private val callsiteCacheMap = new ConcurrentHashMap[BlockId, Option[RDDNode]]()

  private val repeateNodeCacheMap = new ConcurrentHashMap[RDDNode, Option[RDDNode]]()

  def numCrossJobReference(node: RDDNode): Int = {
    val result = dag(node).filter(p => node.jobId != p.jobId)
    logInfo(s"crossJobReference of RDD ${node.rddId}: " +
      s"result: ${result.map {p => (p.rddId, p.jobId)}}")
    result.size
  }

  def findRepeatedNode(findNode: RDDNode, currNode: RDDNode,
                             traversed: mutable.HashSet[RDDNode]): Option[RDDNode] = {
    if (traversed.contains(currNode)) {
      return None
    }

    if (repeateNodeCacheMap.containsKey(findNode)) {
      logDebug(s"Recursive finding done cached repeated node for " +
        s"${findNode.rddId}: ${repeateNodeCacheMap.get(findNode)}")
      return repeateNodeCacheMap.get(findNode)
    }

    traversed.add(currNode)
    var result: Option[RDDNode] = None

    if (reverseDag.contains(currNode) && reverseDag(currNode).nonEmpty) {
      for (parent <- reverseDag(currNode)) {
        if (parent.callsite.equals(findNode.callsite)
          && parent.jobId != findNode.jobId) {
          logDebug(s"Recursive finding done repeated node for " +
            s"${findNode.rddId}: ${parent.rddId}")
          result = Some(parent)
          repeateNodeCacheMap.put(findNode, result)
          return result
        } else if (result.isEmpty) {
          logDebug(s"Recursive finding repeated node for " +
            s"${findNode.rddId}: ${parent.rddId}")
          result = findRepeatedNode(findNode, parent, traversed)
          if (result.isDefined) {
            repeateNodeCacheMap.put(findNode, result)
            return result
          }
        }
      }
    }

    if (!repeateNodeCacheMap.containsKey(findNode)) {
      repeateNodeCacheMap.put(findNode, result)
    }
    result
  }

  def findSameCallsiteParent(findNode: RDDNode, currNode: RDDNode,
                             traversed: mutable.HashSet[RDDNode],
                             index: Int,
                             blockId: BlockId): Option[RDDNode] = {
    if (callsiteCacheMap.containsKey(blockId)) {
      return callsiteCacheMap.get(blockId)
    }

    if (traversed.contains(currNode)) {
      return None
    }

    traversed.add(currNode)

    if (reverseDag.contains(currNode) && reverseDag(currNode).nonEmpty) {
      for (parent <- reverseDag(currNode)) {
        if (parent.callsite.equals(findNode.callsite) && dag(parent).size >= 2) {
          logDebug(s"Finding callsite for $blockId: ${parent}")
          val callsiteBlock = RDDBlockId(parent.rddId, index)
          val size = metricTracker.getBlockSize(callsiteBlock)
          if (size > 0) {
            callsiteCacheMap.put(blockId, Some(parent))
            return Some(parent)
          } else {
            logDebug(s"Size zero callsite for $blockId: ${callsiteBlock}")
            val result = findSameCallsiteParent(findNode, parent, traversed, index, blockId)
            if (result.isDefined) {
              callsiteCacheMap.put(blockId, result)
              return result
            }
          }
        } else {
          logDebug(s"Recursive finding callsite for $blockId: ${parent}, " +
            s"dag(parent): ${dag(parent).size}")
          val result = findSameCallsiteParent(findNode, parent, traversed, index, blockId)
          if (result.isDefined) {
            callsiteCacheMap.put(blockId, result)
            return result
          }
        }
      }
    }

    callsiteCacheMap.put(blockId, None)
    None
  }

  def getRDDNode(blockId: BlockId): RDDNode = {
    val start = System.currentTimeMillis()
    val rddId = blockIdToRDDId(blockId)
    val res = vertices(rddId)
    val elapsed = System.currentTimeMillis() - start
    if (blockId.asRDDId.get.rddId > 100) {
      // logInfo(s"getRDDNode for $blockId took $elapsed ms")
    }
    res
  }

  def getStageRefCnt(blockId: BlockId): Int = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    if (!dag.contains(rddNode)) {
      logWarning(s"No corresponding rdd Node ${rddNode.rddId}")
      return 0
    }

    rddNode.getStages.diff(metricTracker.completedStages).size
  }

  def getCurrentStageUsage(node: RDDNode, blockId: BlockId, taskAttemp: Long): Int = {
    val childNodes = dag(node)

    val sameStageChildren = childNodes
      .filter(n => n.rootStage == node.rootStage &&
      !metricTracker.completedStages.contains(node.rootStage))

    if (sameStageChildren.size > 1) {
      // Check couting
      val attempKey = s"$taskAttemp-${blockId.name}"
      val cnt = if (metricTracker.taskAttempBlockCount.containsKey(attempKey)) {
        metricTracker.taskAttempBlockCount.get(attempKey).get() + 1
      } else {
        1
      }
      sameStageChildren.size - cnt
    } else {
      0
    }
  }

  def getReferenceStages(blockId: BlockId): List[StageDistance] = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    if (!dag.contains(rddNode)) {
      logWarning(s"No corresponding rdd Node exists ${rddNode.rddId}")
      return List.empty
    }


    collectUncachedChildBlocks(rddNode, blockId,
      new mutable.HashSet[Int](), new mutable.HashSet[Int](), 0, 0, 0).values.toList
  }

  private val rddRootStartTimes = new ConcurrentHashMap[Int, Long].asScala

  def getRecompTime(blockId: BlockId, nodeCreatedTime: Long)
  : Long = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    // Collect all cached parents' created times
    // and get the earliest one.
    // If it has no parents, get the start time of the root stage.

    val rootTime = if (rddRootStartTimes.contains(rddId)) {
      rddRootStartTimes(rddId)
    } else {
      rddNode.synchronized {
        rddRootStartTimes.putIfAbsent(rddId, findRootStageStartTimes(rddNode, blockId)._2.min)
        rddRootStartTimes(rddId)
      }
    }

    if (blockId.asRDDId.get.rddId > 100) {
      // logInfo(s"getRecompTime for $blockId took $elapsed ms")
    }

    nodeCreatedTime - rootTime

    /*
    val cachedParentCreatedTime = findRootStageStartTimes(rddNode, blockId)._2.min
    val (parentBlocks, times) = dfsGetCachedParentCreatedTime(blockId)
    val cachedParentCreatedTime = if (times.isEmpty) {
      findRootStageStartTimes(rddNode, blockId)._2.min
    } else {
      times.min
    }

    // Cost of a block denotes computation time of the block,
    // i.e. the time it was created - the time its earliest parent was created
    nodeCreatedTime - cachedParentCreatedTime
    */
  }

  private val cachedMap = new ConcurrentHashMap[Int, (List[RDDNode], Long)]().asScala

  def getLeaf(blockId: BlockId): List[RDDNode] = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    if (!dag.contains(rddNode)) {
      logWarning(s"Not coressponding rdd Node ${rddNode.rddId}")
      return List.empty
    }

    if (cachedMap.contains(rddId)) {
      val v = cachedMap(rddId)
      if (System.currentTimeMillis() - v._2 >= 2000) {
        cachedMap.put(rddId,
          (findLeaf(rddNode, new mutable.HashSet[Int]()), System.currentTimeMillis()))
      }
    } else {
      cachedMap.putIfAbsent(rddId,
        (findLeaf(rddNode, new mutable.HashSet[Int]()), System.currentTimeMillis()))
    }

    cachedMap(rddId)._1
  }

  private def findLeaf(rddNode: RDDNode,
                       visitedRdds: mutable.HashSet[Int]): List[RDDNode] = {

    val l = new mutable.ListBuffer[RDDNode]
    if (visitedRdds.contains(rddNode.rddId)) {
      return l.toList
    } else {
      visitedRdds.add(rddNode.rddId)
    }

    if (!dag.contains(rddNode) || dag(rddNode).isEmpty) {
      // Leaf
      if (!metricTracker.completedStages.contains(rddNode.rootStage)) {
        l.append(rddNode)
      }
      return l.toList
    }

    for (childnode <- dag(rddNode)) {
      // if (!metricTracker.blockStored(childBlockId)) {
      l.appendAll(findLeaf(childnode, visitedRdds))
      // }
    }

    l.toList
  }

  private def collectUncachedChildBlocks(rddNode: RDDNode, blockId: BlockId,
                                         stageSet: mutable.HashSet[Int],
                                         visitedRdds: mutable.HashSet[Int],
                                         distance: Int,
                                         absoluteDistance: Int,
                                         prevcached: Int): Map[Int, StageDistance] = {

    val map = new mutable.HashMap[Int, StageDistance]

    if (visitedRdds.contains(rddNode.rddId)) {
      return map.toMap
    } else {
      visitedRdds.add(rddNode.rddId)
    }

    logInfo(s"Compute cost find child node of ${rddNode}: ${dag(rddNode)}")

    try {
      for (childnode <- dag(rddNode)) {
        val childBlockId = getBlockId(childnode.rddId, blockId)
        var newDist = distance
        var prevc = prevcached
        var absolute = absoluteDistance
        if (!metricTracker.blockStored(childBlockId) && !childnode.shuffled) {

          absolute += 1

          if (map.contains(childnode.rootStage)) {
            // update
            if (map(childnode.rootStage).distance < distance) {
              map(childnode.rootStage).distance = distance
              map(childnode.rootStage).absolute = absolute
            }

            if (map(childnode.rootStage).prevCached > prevc) {
              map(childnode.rootStage).prevCached = prevc
            }
          }


          if (!stageSet.contains(childnode.rootStage) &&
            !metricTracker.completedStages.contains(childnode.rootStage)) {
          // && !metricTracker.stageStartTime.contains(childnode.rootStage)) {
            stageSet.add(childnode.rootStage)
            map.put(childnode.rootStage,
              new StageDistance(childnode.rootStage, distance, absolute, prevc))
            newDist += 1
          }

          if (!metricTracker.completedStages.contains(childnode.rootStage)
            && dag(childnode).size >= 2) {
            // && !metricTracker.stageStartTime.contains(childnode.rootStage)
            // finish
          } else {
            // if (getRefCntRDD(childnode.rddId) < 2) {
            collectUncachedChildBlocks(childnode, blockId,
              stageSet, visitedRdds, newDist, absolute, prevc).foreach {
              entry => map.put(entry._1, entry._2)
            }
          }
          // }
        }
      }
    } catch {
      case e: Exception =>
        // e.printStackTrace()
        logWarning(s"Ehappend !! for finding rdd node ${rddNode.rddId}")
    }

    map.toMap
  }
}

object RDDJobDag extends Logging {

  private def buildReverseDag(dag: Map[RDDNode, mutable.Set[RDDNode]]):
  mutable.Map[RDDNode, mutable.Set[RDDNode]] = {

    val reverseDag = new ConcurrentHashMap[RDDNode, mutable.Set[RDDNode]]().asScala
    dag.foreach {
      pair =>
        val node = pair._1
        val children = pair._2
        children.foreach (child => {
          if (!reverseDag.contains(child)) {
            reverseDag(child) = new mutable.HashSet[RDDNode]()
          }
          reverseDag(child).add(node)
        })
    }
    reverseDag
  }

  def apply(dagPath: String,
            sparkConf: SparkConf,
            metricTracker: MetricTracker): Option[RDDJobDag] = {

    val dag: mutable.Map[RDDNode, mutable.Set[RDDNode]] =
      new ConcurrentHashMap[RDDNode, mutable.Set[RDDNode]]().asScala
    val edges: ListBuffer[(Int, Int)] = mutable.ListBuffer()
    val vertices: mutable.Map[Int, RDDNode] =
      new ConcurrentHashMap[Int, RDDNode]().asScala
    val sampling = sparkConf.get(BlazeParameters.SAMPLING)

    if (sampling) {
      None
    } else if (dagPath.equals("None")) {
      Option(new RDDJobDag(dag, mutable.Map(), metricTracker))
    } else {
      var currentJob = 0

      for (line <- Source.fromFile(dagPath).getLines) {
        val l = line.stripLineEnd
        // parse
        val jsonMap = JSON.parse(l).asInstanceOf[java.util.Map[String, Any]].asScala

        if (jsonMap("Event").equals("SparkListenerJobStart")) {
            currentJob = jsonMap("Job ID").asInstanceOf[Long].toInt
        } else if (jsonMap("Event").equals("SparkListenerStageCompleted")) {
          val stageInfo = jsonMap("Stage Info").asInstanceOf[java.util.Map[Any, Any]].asScala
          logInfo(s"Stage parsing ${stageInfo("Stage ID")}")
          val rdds = stageInfo("RDD Info").asInstanceOf[Array[Object]].toIterator
            // .asSc.java.util.List[java.util.Map[Any, Any]]].asScala

          logInfo(s"rdds: $rdds")
          val stageId = stageInfo("Stage ID").asInstanceOf[Long].toInt
          val parentStages = stageInfo("Parent IDs").asInstanceOf[Array[Object]].toIterator

          // add vertices
          for (rdd_ <- rdds) {
            val rdd = rdd_.asInstanceOf[java.util.Map[Any, Any]].asScala
            val rdd_id = rdd("RDD ID").asInstanceOf[Long].toInt
            val name = rdd("Name").asInstanceOf[String]
            val callsite = rdd("Callsite").asInstanceOf[String]

            logInfo(s"RDD ${rdd_id} Name: ${name}")

            val numCachedPartitions = rdd("Number of Cached Partitions")
              .asInstanceOf[Long].toInt
            val cached = numCachedPartitions > 0
            val parents = rdd("Parent IDs").asInstanceOf[Array[Object]].toIterator

            val rdd_object = new RDDNode(
              rdd_id, stageId, currentJob, name.equals("ShuffledRDD"), callsite, name)
            logInfo(s"RDDID ${rdd_id}, STAGEID: $stageId, jobId: ${currentJob}, " +
              s"name: ${name}, callsite: $callsite")

            if (!dag.contains(rdd_object)) {
              vertices(rdd_id) = rdd_object
              dag(rdd_object) = new mutable.HashSet()
              for (parent_id <- parents) {
                edges.append((parent_id.asInstanceOf[Long].toInt, rdd_id))
              }
            } else {
              dag.keys.filter(p => p.rddId == rdd_object.rddId).foreach {
                p => p.addRefStage(stageId)
              }
            }
          }
        }
      }

      // add edges
      for ((parent_id, child_id) <- edges) {
        val child_rdd_object = vertices(child_id)
       val parent_rdd_object = vertices(parent_id)
        dag(parent_rdd_object).add(child_rdd_object)
      }

      val vv = new mutable.HashSet[RDDNode]()
      dag.keys.foreach {
        // node => logInfo(s"PRDD ${node.rddId}, Edges: ${}, STAGES: ${node.getStages}")
        node => logInfo(s"PRDD ${node.rddId}, Edges: ${dag(node)}")
          if (dag(node).map(n => n.rddId).contains(node.rddId)) {
            throw new RuntimeException(s"RDD ${node.rddId} has cycle ${dag(node)}")
          }

          logInfo(s"Detect to cycle dag for node $node")
          cycleDetection(node, dag, vv, new ListBuffer[RDDNode])
      }

      val rddjobdag = new RDDJobDag(dag, buildReverseDag(dag),
        metricTracker)

      val visited = new mutable.HashSet[RDDNode]()
      rddjobdag.reverseDag.keys.foreach {
        // node => logInfo(s"PRDD ${node.rddId}, Edges: ${}, STAGES: ${node.getStages}")
        node => logInfo(s"ReversePRDD ${node.rddId}, Edges: ${rddjobdag.reverseDag(node)}")
          if (rddjobdag.reverseDag(node).map(n => n.rddId).contains(node.rddId)) {
            throw new RuntimeException(s"RDD ${node.rddId} " +
              s"has cycle ${rddjobdag.reverseDag(node)}")
          }

          logInfo(s"Detect to cycle reverseDag for node $node")
          cycleDetection(node, dag, visited, new ListBuffer[RDDNode])
      }

      // logInfo(s"RddJobDagPrint $rddjobdag")
      Option(rddjobdag)
    }
  }

  private def cycleDetection(root: RDDNode,
                             dag: mutable.Map[RDDNode, mutable.Set[RDDNode]],
                             visited: mutable.Set[RDDNode],
                             path: mutable.ListBuffer[RDDNode]): Boolean = {
    if (!visited.contains(root)) {
      visited.add(root)
      path.append(root)

      dag(root).foreach { node =>

        if (path.contains(node)) {
          throw new RuntimeException(s"Cycle detected... $path")
        }

        cycleDetection(node, dag, visited, path)
      }

      path.remove(path.size-1)
    }

    true
  }

  private def findSameStage(l: mutable.Set[RDDNode], n: RDDNode): Boolean = {
    l.exists(nn => nn.rootStage.equals(n.rootStage))
  }

  class StageDistance(val stageId: Int,
                      var distance: Int,
                      var absolute: Int,
                      var prevCached: Int)

  class BlockCost(val cost: Long,
                  val createTime: Long) {

    override def toString: String = {
      s"$cost"
    }
  }

  class Benefit(val totalReduction: Long,
                val totalSize: Long) {
    def getVal: Double = {
      totalReduction.toDouble / totalSize
    }

    override def toString: String = {
      s"${getVal}/($totalReduction,$totalSize)"
    }
  }
}


