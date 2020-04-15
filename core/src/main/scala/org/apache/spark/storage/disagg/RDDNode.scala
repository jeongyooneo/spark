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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.convert.decorateAsScala._
import scala.collection.{mutable, _}

class RDDNode(val rddId: Int,
              var cached: Boolean,
              var stageId: Int) extends Logging {

  val parents: mutable.ListBuffer[RDDNode] = new mutable.ListBuffer[RDDNode]
  var cachedParents: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()
  var cachedChildren: mutable.Set[RDDNode] = new mutable.HashSet[RDDNode]()

  // <blockId, size>
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

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + rddId
    result
  }

  override def toString: String = {
    s"(rdd: $rddId, cached: $cached, stage: $stageId)"
  }
}
