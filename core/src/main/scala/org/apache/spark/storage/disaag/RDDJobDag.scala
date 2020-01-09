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


import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, RDDBlockId}

import scala.collection.convert.decorateAsScala._
import scala.collection.{mutable, _}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON

class RDDJobDag(val dag: mutable.Map[RDDNode, (mutable.Set[RDDNode], mutable.Set[RDDNode])],
                val edges: ListBuffer[(Int, Int)],
                val vertices: mutable.Map[Int, RDDNode]) extends Logging {

  vertices.foreach { v =>
    val vertex = v._2
    vertex.setCachedParents(RDDJobDag.findCachedParents(vertex))
    vertex.setCachedChildren(RDDJobDag.findCachedChilds(vertex, dag))
    // logInfo(s"RDD $vertex cached parents ${vertex.cachedParents}")
    logInfo(s"RDD $vertex cached children ${vertex.cachedChildren}")
  }

  val startTime = System.currentTimeMillis()

  val blockCost: concurrent.Map[BlockId, Long] = new ConcurrentHashMap[BlockId, Long]().asScala

  def updateCost: Unit = {
    vertices.foreach { v =>
      val vertex = v._2
      vertex.currentStoredBlocks.keys.foreach { key: BlockId =>
        blockCost.put(key, calculateCost(key))
      }
    }

    logInfo(s"BlockCost: ${blockCost}")
  }

  @volatile
  var sortedBlockCost: Option[mutable.ListBuffer[(BlockId, Long)]] = None

  def updateCostAndSort: Unit = {
    val l: mutable.ListBuffer[(BlockId, Long)] = new mutable.ListBuffer[(BlockId, Long)]()

    vertices.foreach { v =>
      val vertex = v._2
      vertex.currentStoredBlocks.keys.foreach { key: BlockId =>
        val cost = calculateCost(key)
        blockCost.put(key, cost)
        l.append((key, cost))
      }
    }

    logInfo(s"SortedBlockCost: ${sortedBlockCost}")
    sortedBlockCost = Some(l.sortWith(_._2 < _._2))
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

  private def dfsCachedParentTimeFind(childBlockId: BlockId): ListBuffer[Long] = {
    val rddId = blockIdToRDDId(childBlockId)
    val rddNode = vertices(rddId)

    if (rddNode.cachedParents.isEmpty) {
      val l: ListBuffer[Long] = mutable.ListBuffer[Long]()
      l.append(startTime)
      l
    } else {
      val l: ListBuffer[Long] = mutable.ListBuffer[Long]()

      for (parent <- rddNode.cachedParents) {
        val parentBlockId = getBlockId(parent.rddId, childBlockId)
        if (!parent.currentStoredBlocks.contains(parentBlockId)) {
          // the parent block is not cached ...
          l.appendAll(dfsCachedParentTimeFind(parentBlockId))
        } else {
          val time = parent.storedBlocksCreatedTime.get(parentBlockId)

          if (time.isDefined) {
            l.append(time.get)
          } else {
            throw new RuntimeException(s"Parent of ${childBlockId} " +
              s"block ($parentBlockId) is not stored.. ")
          }
        }
      }

      l
    }
  }

  def getCost(blockId: BlockId): Long = {
    blockCost.get(blockId) match {
      case None =>
        val cost = calculateCost(blockId)
        blockCost.put(blockId, cost)
        cost
      case Some(cost) =>
        cost
    }
  }

  private def calcChildUncachedBlocks(rddNode: RDDNode, blockId: BlockId): Int = {

    var uncachedSum = 0

    for (cachedChildnode <- rddNode.cachedChildren) {
      val childBlockId = getBlockId(cachedChildnode.rddId, blockId)
      if (!cachedChildnode.currentStoredBlocks.contains(childBlockId)) {
        uncachedSum += 1
        calcChildUncachedBlocks(cachedChildnode, blockId)
      }
    }

    uncachedSum
  }

  def calculateCostToBeStored(blockId: BlockId, nodeCreatedTime: Long): Long = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    var costSum = 0L

    val parentCachedBlockTimes = dfsCachedParentTimeFind(blockId)

    for (parentTime <- parentCachedBlockTimes) {
      costSum += (nodeCreatedTime - parentTime)
    }

    val uncachedChild = calcChildUncachedBlocks(rddNode, blockId)

    logInfo(s"Cost of $blockId: $costSum * $uncachedChild")
    costSum * uncachedChild
  }

  private def calculateCost(blockId: BlockId): Long = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    val nodeCreatedTime = rddNode.storedBlocksCreatedTime.get(blockId).get

    calculateCostToBeStored(blockId, nodeCreatedTime)
  }

  val completedStages: mutable.Set[Int] = new mutable.HashSet[Int]()

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

class RDDNode(val rddId: Int,
              val cached: Boolean,
              val stageId: Int) {

  val parents: mutable.ListBuffer[RDDNode] = new mutable.ListBuffer[RDDNode]

  var cachedParents: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()

  var cachedChildren: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()

  val currentStoredBlocks: concurrent.Map[BlockId, Boolean] =
    new ConcurrentHashMap[BlockId, Boolean]().asScala

  val storedBlocksCreatedTime: concurrent.Map[BlockId, Long] =
    new ConcurrentHashMap[BlockId, Long]().asScala

  def setCachedParents(cp: mutable.Set[RDDNode]): Unit = {
    cachedParents = cp
  }

  def setCachedChildren(cp: mutable.Set[RDDNode]): Unit = {
    cachedChildren = cp
  }

  override def equals(that: Any): Boolean =
    that match
    {
      case that: RDDNode =>
        this.rddId == that.rddId
      case _ => false
    }

  // Defining hashcode method
  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + rddId
    result
  }

  override def toString: String = {
    s"(rdd: $rddId, cached: $cached)"
  }
}

object RDDJobDag extends Logging {
  def apply(filePath: String): Option[RDDJobDag] = {

    if (filePath.equals("??")) {
      Option.empty
    } else {
      val dag: mutable.Map[RDDNode, (mutable.Set[RDDNode], mutable.Set[RDDNode])] = mutable.Map()
      val edges: ListBuffer[(Int, Int)] = mutable.ListBuffer()
      val vertices: mutable.Map[Int, RDDNode] = mutable.Map()


      for (line <- Source.fromFile(filePath).getLines) {
        val l = line.stripLineEnd
        // parse
        val jsonMap = JSON.parseFull(l).getOrElse(0).asInstanceOf[Map[String, Any]]

        if (jsonMap("Event").equals("SparkListenerStageCompleted")) {
          val stageInfo = jsonMap("Stage Info").asInstanceOf[Map[Any, Any]]
          logInfo(s"Stage parsing ${stageInfo("Stage ID")}")
          val rdds = stageInfo("RDD Info").asInstanceOf[List[Map[Any, Any]]]
          val stageId = stageInfo("Stage ID").asInstanceOf[Double].toInt

          // add vertices
          for (rdd <- rdds) {
            val rdd_id = rdd("RDD ID").asInstanceOf[Double].toInt
            val numCachedPartitions = rdd("Number of Cached Partitions")
              .asInstanceOf[Double].toInt
            val cached = numCachedPartitions > 0
            val parents = rdd("Parent IDs").asInstanceOf[List[Double]]
            val rdd_object = new RDDNode(rdd_id, cached, stageId)

            if (!dag.contains(rdd_object)) {
              vertices(rdd_id) = rdd_object
              dag(rdd_object) = (new mutable.HashSet(), new mutable.HashSet())
              for (parent_id: Double <- parents) {
                edges.append((parent_id.toInt, rdd_id))
              }
            }
          }
        }
      }

      // add edges
      for ((parent_id, child_id) <- edges) {
        val child_rdd_object = vertices(child_id)
        val parent_rdd_object = vertices(parent_id)
        dag(parent_rdd_object)._1.add(child_rdd_object)
        child_rdd_object.parents.append(parent_rdd_object)
      }

      for (node <- dag.keys) {
        val (child, child_stage) = dag(node)

        for (child_node <- child) {
          if (!findSameStage(child_stage, child_node)) {
            child_stage.add(child_node)
          }
        }

        dag(node) = (child, child_stage)
      }

      Option(new RDDJobDag(dag, edges, vertices))
    }
  }

  private def findSameStage(l: mutable.Set[RDDNode], n: RDDNode): Boolean = {
    l.exists(nn => nn.stageId.equals(n.stageId))
  }

  def findCachedChilds(parent: RDDNode,
                       dag: mutable.Map[RDDNode,
                         (mutable.Set[RDDNode], mutable.Set[RDDNode])]): mutable.Set[RDDNode] = {

    def find(parent: RDDNode, childNode: RDDNode): mutable.Set[RDDNode] = {
      if (childNode.cached) {
        val l = new mutable.HashSet[RDDNode]
        l.add(childNode)
        l
      } else {
        var l: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
        for (child <- dag(childNode)._1) {
          val n = find(childNode, child)
          l = l.union(n)
        }
        l
      }
    }

    var set: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
    for (child <- dag(parent)._1) {
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
}
