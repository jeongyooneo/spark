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

  def onlineUpdate(newDag: Map[RDDNode, mutable.Set[RDDNode]]): Unit = {
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

      // logInfo(s"Print DAG: $dag")
      // logInfo(s"Print ReverseDAG: $reverseDag")
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

  private def dfsGetBlockElapsedTime(myRDD: Int,
                                     childBlockId: BlockId,
                                     nodeCreatedTime: Long,
                                     visited: mutable.Set[RDDNode],
                                     tSum: Long):
  (ListBuffer[BlockId], ListBuffer[Long]) = {

    val b: ListBuffer[BlockId] = mutable.ListBuffer[BlockId]()
    val l: ListBuffer[Long] = mutable.ListBuffer[Long]()

    val rddId = blockIdToRDDId(childBlockId)
    val rddNode = vertices(rddId)

    if (visited.contains(rddNode)) {
      return (b, l)
    }

    visited.add(rddNode)

    val myKey = s"${childBlockId.name}-${childBlockId.name}"
    val timeSum = if (metricTracker.blockElapsedTimeMap.containsKey(myKey)) {
      tSum + metricTracker.blockElapsedTimeMap.get(myKey)
    } else {
      tSum
    }

    if (!reverseDag.contains(rddNode) || reverseDag(rddNode).isEmpty) {
      // find root stage!!
      if (rddNode.shuffled) {
        b.append(getBlockId(rddNode.rddId, childBlockId))
        l.append(timeSum)
      } else {
        // This is root RDD that reads input!
        // We increase the time because it causes memory pressure.
        b.append(getBlockId(rddNode.rddId, childBlockId))
        l.append(timeSum + 100000)
      }
    } else {
      for (parent <- reverseDag(rddNode)) {
        val parentBlockId = getBlockId(parent.rddId, childBlockId)
        val key = s"${parentBlockId.name}-${childBlockId.name}"
        val elapsedTimeFromParent = metricTracker.blockElapsedTimeMap.get(key)
        val added = timeSum + elapsedTimeFromParent

        if (metricTracker.blockStored(parentBlockId)) {
          b.append(parentBlockId)
          if (metricTracker.localDiskStoredBlocksMap.containsKey(parentBlockId)) {
            // If it is cached, we should read it from mem or disk
            // If it is stored in disk, we should add disk read overhead
            val diskoverhead = BlazeParameters.readThp * metricTracker.getBlockSize(parentBlockId)
            l.append(added + diskoverhead.toLong)
          } else {
            l.append(added)
          }
        } else {
          val (bb, ll) =
            dfsGetBlockElapsedTime(myRDD, parentBlockId, nodeCreatedTime, visited, added)
          b.appendAll(bb)
          l.appendAll(ll)
        }
      }
    }

    (b, l)
  }

  def blockCompTime(blockId: BlockId, nodeCreatedTime: Long): Long = {
    val rddId = blockIdToRDDId(blockId)
    val (parentBlocks, times) =
      dfsGetBlockElapsedTime(rddId, blockId,
        nodeCreatedTime, new mutable.HashSet[RDDNode](), 0L)

    val t = times.max
    // logInfo(s"BlockComptTime of ${blockId}: ${parentBlocks}, " + s"${times}")
    t
  }

  def getRefCntRDD(rddId: Int): Int = {
    val rddNode = vertices(rddId)
    val edges = dag(rddNode)

    edges.map(p => p.rootStage).toSet.size
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

  def getRefCnt(blockId: BlockId): Int = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    if (!dag.contains(rddNode)) {
      logWarning(s"Not coressponding rdd Node ${rddNode.rddId}")
      return 0
    }

    val uncachedChildNum = collectUncachedChildBlocks(rddNode, blockId,
      new mutable.HashSet[Int](), new mutable.HashSet[Int](), 0, 0, 0).size

    uncachedChildNum
  }

  def getRDDNode(rddId: Int): RDDNode = {
    vertices(rddId)
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
            stageSet.add(childnode.rootStage)
            map.put(childnode.rootStage,
              new StageDistance(childnode.rootStage, distance, absolute, prevc))
            newDist += 1
          }

          if (!metricTracker.completedStages.contains(childnode.rootStage)
            && dag(childnode).size >= 2) {
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
      for (line <- Source.fromFile(dagPath).getLines) {
        val l = line.stripLineEnd
        // parse
        val jsonMap = JSON.parse(l).asInstanceOf[java.util.Map[String, Any]].asScala

        if (jsonMap("Event").equals("SparkListenerStageCompleted")) {
          val stageInfo = jsonMap("Stage Info").asInstanceOf[java.util.Map[Any, Any]].asScala
          logInfo(s"Stage parsing ${stageInfo("Stage ID")}")
          val rdds = stageInfo("RDD Info").asInstanceOf[Array[Object]].toIterator
            // .asSc.java.util.List[java.util.Map[Any, Any]]].asScala

          logInfo(s"rdds: $rdds")
          val stageId = stageInfo("Stage ID").asInstanceOf[Long].toInt

          // add vertices
          for (rdd_ <- rdds) {
            val rdd = rdd_.asInstanceOf[java.util.Map[Any, Any]].asScala
            val rdd_id = rdd("RDD ID").asInstanceOf[Long].toInt
            val name = rdd("Name").asInstanceOf[String]

            val numCachedPartitions = rdd("Number of Cached Partitions")
              .asInstanceOf[Long].toInt
            val cached = numCachedPartitions > 0
            val parents = rdd("Parent IDs").asInstanceOf[Array[Object]].toIterator

            val rdd_object = new RDDNode(rdd_id, stageId, name.equals("ShuffledRDD"))
            logInfo(s"RDDID ${rdd_id}, STAGEID: $stageId")

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
        node => logInfo(s"ReversePRDD ${node.rddId}, Edges: ${dag(node)}")
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


