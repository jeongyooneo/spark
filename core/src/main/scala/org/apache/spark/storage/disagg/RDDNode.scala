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

import org.apache.spark.internal.Logging

import scala.collection.mutable

class RDDNode(val rddId: Int,
              val stageId: Int,
              val jobId: Int,
              val shuffled: Boolean,
              val callsite: String,
              val name: String) extends Logging {

  private val stageIds = new mutable.HashSet[Int]()

  var crossReferenced = false

  if (stageId >= 0) {
    stageIds.add(stageId)
  }

  // val parents: mutable.ListBuffer[RDDNode] = new mutable.ListBuffer[RDDNode]
  // var cachedParents: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()
  // var cachedChildren: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()

  // <blockId, size>
  // val currentStoredBlocks: concurrent.Map[BlockId, Boolean] =
  //  new ConcurrentHashMap[BlockId, Boolean]().asScala

  // val storedBlocksCreatedTime: concurrent.Map[BlockId, Long] =
  //  new ConcurrentHashMap[BlockId, Long]().asScala

  private var root = if (stageId >= 0) {
    stageId
  } else {
    100000
  }

  def getStages: Set[Int] = synchronized {
    stageIds.toSet
  }

  def addRefStages(s: Set[Int]): Unit = synchronized {
    s.foreach { ss => stageIds.add(ss) }
  }

  val refJobs = new mutable.HashSet[Int]()

  def addRefStage(stageId: Int, jid: Int): Unit = synchronized {
    stageIds.add(stageId)
    refJobs.add(jid)
    if (root > stageId) {
      root = stageId
    }
  }

  def rootStage: Int = synchronized {
    root
  }

  override def equals(that: Any): Boolean =
    that match
    {
      case that: RDDNode =>
        this.rddId == that.rddId
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + rddId
    result
  }

  override def toString: String = {
    // s"(rdd: $rddId, stage: $stageIds)"
    s"(rdd: $rddId, job: ${jobId}, stage: ${rootStage}, " +
      s"cf: ${crossReferenced}, callsite: ${callsite})"
  }
}
