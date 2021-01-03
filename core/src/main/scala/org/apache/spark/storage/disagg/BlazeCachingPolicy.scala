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

private[spark] class BlazeCachingPolicy(val rddJobDag: RDDJobDag)
  extends CachingPolicy with Logging {

  def isRDDNodeCached(rddId: Int): Option[Boolean] = {
    if (rddJobDag.containsRDD(rddId)) {
      val refcnt = rddJobDag.getRefCntRDD(rddId)

      val rddNode = rddJobDag.getRDDNode(rddId)
      val repeat = rddJobDag.findRepeatedNode(rddNode, rddNode, new mutable.HashSet[RDDNode]())

      logInfo(s"Caching decision for ${rddNode}")

      repeat match {
        case Some(rnode) =>
          // consider repeated pattern
          logInfo(s"Caching decision for ${rddNode} rnode ${rnode}")
          if (refcnt != rddJobDag.dag(rnode).size) {
            logInfo(s"Caching decision for ${rddNode}, rnode ${rnode} edge size ${refcnt}, " +
              s" different size ${rddJobDag.dag(rnode)}")
            rddJobDag.dag(rnode).size
          } else {
            logInfo(s"Caching decision for ${rddNode}, rnode ${rnode} edge size ${refcnt}")
            refcnt
          }
        case None =>
          logInfo(s"Caching decision for ${rddNode}, edge size ${refcnt}")
          refcnt
      }      // logInfo(s"Reference count of RDD $rddId: $refcnt")

      Some(refcnt > 1)
    } else {
      logInfo(s"Caching decision for rdd $rddId no contains...!!")
      None
    }
  }
}


