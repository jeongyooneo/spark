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

import org.apache.spark.SparkConf

private[spark] trait CachingPolicy {

  def isRDDNodeCached(rddId: Int): Option[Boolean]
}

object CachingPolicy {
  def apply(rddJobDag: Option[RDDJobDag],
            sparkConf: SparkConf,
            metricTracker: MetricTracker): CachingPolicy = {

    val policy = sparkConf.get(BlazeParameters.CACHING_POLICY)

    if (policy.equals("Blaze")) {
      if (rddJobDag.isDefined) {
        new BlazeCachingPolicy(rddJobDag.get, metricTracker)
      } else {
        new RandomCachingPolicy(sparkConf.get(BlazeParameters.RAND_CACHING_POLICY_PARAM))
      }
    } else if (policy.equals("Random")) {
      new RandomCachingPolicy(sparkConf.get(BlazeParameters.RAND_CACHING_POLICY_PARAM))
    } else {
      throw new RuntimeException(s"No caching policy for $policy")
    }
  }
}


