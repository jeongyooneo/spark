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


import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON


class RDDJobDag(val dag: mutable.Map[RDDNode, (mutable.Set[RDDNode], mutable.Set[RDDNode])],
                val edges: ListBuffer[(Int, Int)],
                val vertices: mutable.Map[Int, RDDNode]) extends Logging {

  vertices.foreach { v =>
    val vertex = v._2
    vertex.setCachedParents(RDDJobDag.findCachedParents(vertex))
    logInfo(s"RDD $vertex cached parents ${vertex.cachedParents}")
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

  def setCreatedTimeForRDD(blockId: BlockId): Unit = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)
    rddNode.synchronized {
      if (rddNode.createdTime.get() < System.currentTimeMillis()) {
        rddNode.createdTime.set(System.currentTimeMillis())
      }
    }
  }

  def calculateCost(blockId: BlockId, nodeCreatedTime: Long): Long = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    var parentCreatedTime = Long.MaxValue
    for (parent <- rddNode.parents) {
      if (parent.createdTime.get() < parentCreatedTime) {
        parentCreatedTime = parent.createdTime.get()
      }
    }

    (nodeCreatedTime - parentCreatedTime) * dag(rddNode)._2.size
  }

  def calculateCost(blockId: BlockId): Long = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = vertices(rddId)

    val nodeCreatedTime = rddNode.createdTime.get()

    var parentCreatedTime = Long.MaxValue
    for (parent <- rddNode.cachedParents) {
      if (parent.createdTime.get() < parentCreatedTime) {
        parentCreatedTime = parent.createdTime.get()
      }
    }

    if (parentCreatedTime == Long.MaxValue) {
      // this means that there is no cached parent RDD
      nodeCreatedTime
    } else {
      Math.max(0L, nodeCreatedTime - parentCreatedTime)
    }
  }
}

class RDDNode(val rddId: Int,
              val cached: Boolean,
              val stageId: Int) {

  val parents: mutable.ListBuffer[RDDNode] = new mutable.ListBuffer[RDDNode]

  var createdTime: AtomicLong = new AtomicLong(0)

  var cachedParents: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()

  def setCachedParents(cp: mutable.Set[RDDNode]): Unit = {
    cachedParents = cp
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
          val stageId = stageInfo("Stage ID").asInstanceOf[Int]

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
    l.filter(n => n.stageId == n.stageId).nonEmpty
  }

  def findCachedParents(child: RDDNode): mutable.Set[RDDNode] = {

    def find(child: RDDNode, parentNode: RDDNode): mutable.Set[RDDNode] = {
      if (parentNode.cached) {
        val l = new mutable.HashSet[RDDNode]
        l.add(parentNode)
        l
      } else {
        val l: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]
        for (parent <- parentNode.parents) {
          val n = find(parentNode, parent)
          l.union(n)
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
