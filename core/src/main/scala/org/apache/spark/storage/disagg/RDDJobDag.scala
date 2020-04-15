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
import org.apache.spark.storage.disagg.RDDJobDag.{Benefit, BlockCost}
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.mortbay.util.ajax.JSON

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}
import scala.io.Source

class RDDJobDag(val dag: mutable.Map[RDDNode, mutable.Set[RDDNode]],
                val reverseDag: mutable.Map[RDDNode, mutable.Set[RDDNode]],
                val autocaching: Boolean,
                val evictionPolicy: String) extends Logging {

  val blockCost: concurrent.Map[BlockId, BlockCost] =
    new ConcurrentHashMap[BlockId, BlockCost]().asScala
  @volatile
  var sortedBlockCost: Option[mutable.ListBuffer[(BlockId, BlockCost)]] = None

  val blockCreatedTimes: concurrent.Map[BlockId, Long] =
    new ConcurrentHashMap[BlockId, Long]().asScala

  val stageStartTime: concurrent.Map[Int, Long] =
    new ConcurrentHashMap[Int, Long]().asScala

  val taskStartTime: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala

  val completedStages: mutable.Set[Int] = new mutable.HashSet[Int]()

  val dagChanged = new AtomicBoolean(true)

  val lrcBased = evictionPolicy.contains("LRC")

  private var prevVertices: Map[Int, RDDNode] = new mutable.HashMap[Int, RDDNode]()
  def vertices: Map[Int, RDDNode] = {
    dagChanged.synchronized {
      if (dagChanged.get()) {
        prevVertices = dag.keySet.map(node => (node.rddId, node)).toMap
        dagChanged.set(false)
        prevVertices
      } else {
        prevVertices
      }
    }
  }

  if (autocaching) {
    // clear cached rdds
    vertices.foreach { v =>
      if (v._2.cached) {
        logInfo(s"Prev cached RDD: ${v._2.rddId}")
        v._2.cached = false
      }
    }

    // re-set cached rdds
    vertices.foreach { v =>
      if (dag(v._2).size > 1) {
        logInfo(s"Num of children of RDD ${v._2.rddId}: ${dag(v._2).size}, cache!!")
        v._2.cached = true
      }
    }
  }

  // Set cached parents and children for each vertices
  vertices.foreach { v =>
    val vertex = v._2
    vertex.setCachedParents(RDDJobDag.findCachedParents(vertex))
    vertex.setCachedChildren(RDDJobDag.findCachedChildren(vertex, dag))
    // logInfo(s"RDD $vertex cached parents ${vertex.cachedParents}")
    logInfo(s"RDD $vertex cached children ${vertex.cachedChildren}")
  }

  def getCachedRDDs(): Iterable[Int] = {
    vertices.filter(node => node._2.cached).map(node => node._2.rddId)
  }

  def updateCost: Unit = {
    vertices.foreach { v =>
      val vertex = v._2
      vertex.currentStoredBlocks.keys.foreach { key: BlockId =>
        blockCost.put(key, calculateCost(key))
      }
    }

    logInfo(s"BlockCost: ${blockCost}")
  }

  def onlineUpdate(newDag: Map[RDDNode, mutable.Set[RDDNode]]): Unit = {
    dagChanged.synchronized {
      // update dag

      newDag.foreach { pair =>

        val parent = pair._1
        pair._2.foreach {
          child =>
            if (!reverseDag.contains(child)) {
              reverseDag(child) = new mutable.HashSet[RDDNode]()
            }
            reverseDag(child).add(parent)
        }

        if (!dag.contains(pair._1)) {
          logInfo(s"New vertex is created ${pair._1}->${pair._2}")
          dag.put(pair._1, pair._2)
          dagChanged.set(true)
        } else {
          if (!pair._2.subsetOf(dag(pair._1))) {
            pair._2.foreach { dag(pair._1).add(_) }
            logInfo(s"New edges are created for RDD " +
              s"${pair._1.rddId}, ${dag(pair._1)}")
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
        dagChanged.set(true)
        unmatchedRDDs.foreach {
          pair =>
            val child = pair._1
            val parents = pair._2
            val prevParents = reverseDag(child)
            val diff = prevParents.diff(parents)
            diff.foreach {
              prevParent =>
                dag(prevParent).remove(child)
            }
        }
      }
    }
  }

  def getZeroCostRDDs: Set[Int] = {
    updateCostAndSort

    val zeros: mutable.HashSet[Int] = new mutable.HashSet[Int]()
    val nonzeros: mutable.HashSet[Int] = new mutable.HashSet[Int]()

    sortedBlockCost match {
      case None =>
      case Some(l) =>
        l.foreach {
          pair =>
            if (pair._2.cost <= 0) {
              zeros.add(pair._1.asRDDId.get.rddId)
            } else {
              nonzeros.add(pair._1.asRDDId.get.rddId)
            }
        }
    }

    zeros.diff(nonzeros)
  }

  // Return benefit = (total importance/total size)
  def updateCostAndSort: Unit = synchronized {
    val l: mutable.ListBuffer[(BlockId, BlockCost)] = new mutable.ListBuffer[(BlockId, BlockCost)]()

    val blockBenefitList: mutable.ListBuffer[(BlockId, Benefit)] =
      new mutable.ListBuffer[(BlockId, Benefit)]()

    var totalImportance: Long = 0L
    var totalSize: Long = 0L

    vertices.foreach { v =>
      val vertex = v._2
      vertex.currentStoredBlocks.foreach { entry: (BlockId, Boolean) =>
        val blockId = entry._1
        // val size = entry._2 / 1000

        val cost = calculateCost(blockId)
        blockCost.put(blockId, cost)

        l.append((blockId, cost))

        // blockBenefitList.append((blockId, new Benefit(cost.cost, size)))

        // totalImportance += cost.cost
        // totalSize += size
      }
    }

    totalSize = Math.max(1, totalSize)
    sortedBlockCost = Some(l.sortWith(_._2.cost < _._2.cost))
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

  def setStoredBlocksCreatedTime(blockId: BlockId): Unit = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    if (!rddNode.storedBlocksCreatedTime.contains(blockId)) {
      rddNode.storedBlocksCreatedTime.put(blockId, System.currentTimeMillis())
    }
  }

  def storingBlock(blockId: BlockId): Unit = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    rddNode.currentStoredBlocks.put(blockId, true)
    if (!rddNode.storedBlocksCreatedTime.contains(blockId)) {
      rddNode.storedBlocksCreatedTime.put(blockId, System.currentTimeMillis())
    }
  }

  def removingBlock(blockId: BlockId): Unit = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    rddNode.currentStoredBlocks.remove(blockId)
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

  private def getTaskStartTimeFromBlockId(stageId: Int, blockId: BlockId): Option[Long] = {
    val index = blockId.name.split("_")(2).toInt

    for(i <- 0 to index) {
      val taskId = s"$stageId-$i-0"
      taskStartTime.get(taskId) match {
        case None =>
          //  do nothing
        case Some(startTime) =>
          return Some(startTime)
      }
    }

    None
  }

  private def findRootStageStartTimes(rddNode: RDDNode, blockId: BlockId):
  (ListBuffer[BlockId], ListBuffer[Long]) = {

    val b: ListBuffer[BlockId] = mutable.ListBuffer[BlockId]()
    val l: ListBuffer[Long] = mutable.ListBuffer[Long]()

    if (rddNode.parents.isEmpty) {
      val rootTaskId = getTaskId(rddNode.stageId, blockId)
      taskStartTime.get(rootTaskId) match {
        case None =>
          getTaskStartTimeFromBlockId(rddNode.stageId, blockId) match {
            case None =>
              b.append(blockId)
              l.append(stageStartTime(rddNode.stageId))
            case Some(startTime) =>
              b.append(blockId)
              l.append(startTime)
          }
        case Some(startTime) =>
          b.append(blockId)
          l.append(startTime)
      }
    } else {
      for (parent <- rddNode.parents) {
        val (bb, ll) = findRootStageStartTimes(parent, blockId)
        b.appendAll(bb)
        l.appendAll(ll)
      }
    }

    (b, l)
  }

  def setBlockCreatedTime(blockId: BlockId): Unit = {
    if (!blockCreatedTimes.contains(blockId)) {
      blockCreatedTimes.putIfAbsent(blockId, System.currentTimeMillis())
    }
  }

  private def getBlockCreatedTime(rddId: Int, childBlockId: BlockId): Option[Long] = {
    val index = childBlockId.name.split("_")(2).toInt
    RDDBlockId(rddId, index)

    for (i <- 9 to index) {
      val bid = RDDBlockId(rddId, i)
      val time = blockCreatedTimes.get(bid)
      if (time.isDefined) {
        return time
      }
    }

    return None
  }

  private def dfsGetCachedParentCreatedTime(childBlockId: BlockId):
  (ListBuffer[BlockId], ListBuffer[Long]) = {

    val rddId = blockIdToRDDId(childBlockId)
    val rddNode = vertices(rddId)

    if (rddNode.cachedParents.isEmpty) {
      // find root stage!!
      findRootStageStartTimes(rddNode, childBlockId)
    } else {
      val l: ListBuffer[Long] = mutable.ListBuffer[Long]()
      val b: ListBuffer[BlockId] = mutable.ListBuffer[BlockId]()

      for (parent <- rddNode.cachedParents) {
        val parentBlockId = getBlockId(parent.rddId, childBlockId)
        val time = blockCreatedTimes.get(parentBlockId)

        if (time.isDefined) {
          b.append(parentBlockId)
          l.append(time.get)
        } else {
          getBlockCreatedTime(parent.rddId, childBlockId) match {
            case None =>
              dfsGetCachedParentCreatedTime(parentBlockId)
              // throw new RuntimeException(s"Parent of ${childBlockId} " +
              //  s"block ($parentBlockId) is not stored.. ")
            case Some(t) =>
              b.append(parentBlockId)
              l.append(t)
          }
        }

        /*
        if (!parent.currentStoredBlocks.contains(parentBlockId)) {
          // the parent block is not cached ...
          l.appendAll(dfsGetCachedParentCreatedTime(parentBlockId))
        } else {
          val time = parent.storedBlocksCreatedTime.get(parentBlockId)

          if (time.isDefined) {
            l.append(time.get)
          } else {
            throw new RuntimeException(s"Parent of ${childBlockId} " +
              s"block ($parentBlockId) is not stored.. ")
          }
        }
        */
      }

      (b, l)
    }
  }

  // Unused
  def getBlockCost(blockId: BlockId): Long = {
    blockCost.get(blockId) match {
      case None =>
        val cost = calculateCost(blockId)
        blockCost.put(blockId, cost)
        cost.cost
      case Some(cost) =>
        cost.cost
    }
  }

  private def calculateCost(blockId: BlockId): BlockCost = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    val nodeCreatedTime = rddNode.storedBlocksCreatedTime.get(blockId).get

    calculateCostToBeStored(blockId, nodeCreatedTime)
  }

  def blockCompTime(blockId: BlockId, nodeCreatedTime: Long): Long = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    var cost = 0L

    val (parentBlocks, times) = dfsGetCachedParentCreatedTime(blockId)
    val parentCachedBlockTime = if (times.isEmpty) {
      findRootStageStartTimes(rddNode, blockId)._2.min
    } else {
      times.min
    }

    cost += (nodeCreatedTime - parentCachedBlockTime)
    cost
  }

  def calculateCostToBeStored(blockId: BlockId, nodeCreatedTime: Long): BlockCost = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    var cost = 0L

    // Collect all cached parents' created times
    // and get the earliest one.
    // If it has no parents, get the start time of the root stage.
    val (parentBlocks, times) = dfsGetCachedParentCreatedTime(blockId)
    val cachedParentCreatedTime = if (times.isEmpty) {
      findRootStageStartTimes(rddNode, blockId)._2.min
    } else {
      times.min
    }

    // Cost of a block denotes computation time of the block,
    // i.e. the time it was created - the time its earliest parent was created
    cost += (nodeCreatedTime - cachedParentCreatedTime)

    // logInfo(s"parent blocks for $blockId: $parentBlocks")

    val uncachedChildNum = collectUncachedChildBlocks(rddNode, blockId,
      new mutable.HashSet[Int](), new mutable.HashSet[Int]())

    // logInfo(s"child blocks for $blockId: $uncachedChildList")

    if (lrcBased) {
      new BlockCost(
        uncachedChildNum,
        nodeCreatedTime,
        parentBlocks,
        times)
    } else {
      new BlockCost(
        cost * uncachedChildNum,
        nodeCreatedTime,
        parentBlocks,
        times)
    }
  }

  private def collectUncachedChildBlocks(rddNode: RDDNode, blockId: BlockId,
                                      stageSet: mutable.HashSet[Int],
                                      visitedRdds: mutable.HashSet[Int]): Int = {

    var cnt = 0

    // logInfo(s"blockId: $blockId, " +
    //  s"rddNode: ${rddNode.rddId}/${rddNode.stageId}, stageSet: $stageSet")

    if (visitedRdds.contains(rddNode.rddId)) {
      return cnt
    } else {
      visitedRdds.add(rddNode.rddId)
    }

    for (childnode <- dag(rddNode)) {
      val childBlockId = getBlockId(childnode.rddId, blockId)
      if (!childnode.currentStoredBlocks.contains(childBlockId)) {
        if (!stageSet.contains(childnode.stageId) &&
          !completedStages.contains(childnode.stageId)) {
          cnt += 1
          stageSet.add(childnode.stageId)
        }
        cnt += collectUncachedChildBlocks(childnode, blockId, stageSet, visitedRdds)
      }
    }

    cnt
  }

  def taskStarted(taskId: String): Unit = synchronized {
    taskStartTime.putIfAbsent(taskId, System.currentTimeMillis())
  }

  def stageSubmitted(stageId: Int): Unit = {
    stageStartTime.putIfAbsent(stageId, System.currentTimeMillis())
  }

  def removeCompletedStageNode(stageId: Int): Unit = {
    completedStages.add(stageId)
    /*
    for (key <- dag.keys) {
      val (edge, stage_removed_edge) = dag(key)
      stage_removed_edge.synchronized {
        val same_stage_id_edges = stage_removed_edge.filter(n => n.stageId.equals(stageId))
        for (edge_node <- same_stage_id_edges) {
          logInfo(s"Stage completed.. remove Stage ${stageId} nodes from ${key}")
          stage_removed_edge.remove(edge_node)
        }
      }
    }
    */
  }
}

object RDDJobDag extends Logging {

  private def buildReverseDag(dag: Map[RDDNode, mutable.Set[RDDNode]]):
  mutable.Map[RDDNode, mutable.Set[RDDNode]] = {

    val reverseDag = new mutable.HashMap[RDDNode, mutable.Set[RDDNode]]
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
            sparkConf: SparkConf): Option[RDDJobDag] = {

    val dag: mutable.Map[RDDNode, mutable.Set[RDDNode]] = mutable.Map()
    val edges: ListBuffer[(Int, Int)] = mutable.ListBuffer()
    val vertices: mutable.Map[Int, RDDNode] = mutable.Map()
    val policy = sparkConf.get("spark.disagg.evictpolicy", "None")

    if (dagPath.equals("None")) {
      Option(new RDDJobDag(dag, mutable.Map(), false, policy))
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
            val numCachedPartitions = rdd("Number of Cached Partitions")
              .asInstanceOf[Long].toInt
            val cached = numCachedPartitions > 0
            val parents = rdd("Parent IDs").asInstanceOf[Array[Object]].toIterator
            val rdd_object = new RDDNode(rdd_id, cached, stageId)

            if (!dag.contains(rdd_object)) {
              vertices(rdd_id) = rdd_object
              dag(rdd_object) = new mutable.HashSet()
              for (parent_id <- parents) {
                edges.append((parent_id.asInstanceOf[Long].toInt, rdd_id))
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
        child_rdd_object.parents.append(parent_rdd_object)
      }

      Option(new RDDJobDag(dag, buildReverseDag(dag),
        sparkConf.getBoolean("spark.disagg.autocaching", false), policy))
    }
  }

  private def findSameStage(l: mutable.Set[RDDNode], n: RDDNode): Boolean = {
    l.exists(nn => nn.stageId.equals(n.stageId))
  }

  def findCachedChildren(parent: RDDNode,
                         dag: mutable.Map[RDDNode, mutable.Set[RDDNode]]): mutable.Set[RDDNode] = {

    def find(parent: RDDNode, childNode: RDDNode): mutable.Set[RDDNode] = {
      if (childNode.cached) {
        val l = new mutable.HashSet[RDDNode]
        l.add(childNode)
        l
      } else {
        var l: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
        for (child <- dag(childNode)) {
          val n = find(childNode, child)
          l = l.union(n)
        }
        l
      }
    }

    var set: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
    for (child <- dag(parent)) {
      val v = find(parent, child)
      set = set.union(v)
      logInfo(s"Find cached child of ${parent}/$child $v, $set")
    }

    set
  }

  def findCachedParents(child: RDDNode): mutable.Set[RDDNode] = {

    def find(child: RDDNode, parentNode: RDDNode): mutable.Set[RDDNode] = {
      if (parentNode.cached) {
        val l = new mutable.HashSet[RDDNode]
        l.add(parentNode)
        l
      } else {
        var l: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
        for (parent <- parentNode.parents) {
          val n = find(parentNode, parent)
          l = l.union(n)
        }
        l
      }
    }

    var set: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
    for (parent <- child.parents) {
      val v = find(child, parent)
      set = set.union(v)
      // logInfo(s"Find cached parent of ${child}: $v, $set")
    }

    set
  }

  class BlockCost(val cost: Long,
                  val createTime: Long,
                  val rootParentBlock: ListBuffer[BlockId],
                  val rootParentStartTime: ListBuffer[Long]) {

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
