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

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

private[spark] object BlazeParameters extends Logging {

  // TODO: Remove everything but 'testable knobs'

  val readThp = 3000.0 / (600 * 1024 * 1024)
  val writeThp = 3000.0 / (600 * 1024 * 1024)

  private[spark] val EXECUTOR_DISK_CAPACITY_MB = ConfigBuilder("spark.blaze.executorDiskCapacity")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("0g")

  private[spark] val JOB_DAG_PATH = ConfigBuilder("spark.blaze.dagpath")
    .stringConf
    .createWithDefault("??")

  private[spark] val AUTOCACHING = ConfigBuilder("spark.blaze.autocaching")
    .booleanConf
    .createWithDefault(true)

  private[spark] val AUTOUNPERSIST = ConfigBuilder("spark.blaze.autounpersist")
    .booleanConf
    .createWithDefault(true)

  private[spark] val ENABLE_DISKSTORE = ConfigBuilder("spark.blaze.enableDiskStore")
    .booleanConf
    .createWithDefault(false)

  private[spark] val DISK_LOCALITY_UNAWARE = ConfigBuilder("spark.blaze.diskLocalityUnaware")
    .booleanConf
    .createWithDefault(false)

  private[spark] val CACHING_POLICY = ConfigBuilder("spark.blaze.cachingpolicy")
    .stringConf
    .createWithDefault("Blaze")

  private[spark] val EVICTION_POLICY = ConfigBuilder("spark.blaze.evictionpolicy")
    .stringConf
    .createWithDefault("Default")

  private[spark] val RAND_CACHING_POLICY_PARAM =
    ConfigBuilder("spark.blaze.cachingpolicy.random.percentage")
    .doubleConf
    .createWithDefault(0.2)

  private[spark] val SAMPLING = ConfigBuilder("spark.blaze.sampledRun")
    .booleanConf
    .createWithDefault(false)

  private[spark] val DISABLE_LOCAL_CACHING = ConfigBuilder("spark.blaze.disableLocalCaching")
    .booleanConf
    .createWithDefault(false)

  private[spark] val COST_FUNCTION = ConfigBuilder("spark.blaze.costfunction")
    .stringConf
    .createWithDefault("None")

  private[spark] val PROMOTION_RATIO = ConfigBuilder("spark.blaze.promotionRatio")
    .doubleConf
    .createWithDefault(0.3)

  private[spark] val DISABLE_PROMOTE = ConfigBuilder("spark.blaze.promote.disable")
    .booleanConf
    .createWithDefault(false)

   private[spark] val ALWAYS_CACHE = ConfigBuilder("spark.blaze.cachingUnconditionally")
    .booleanConf
    .createWithDefault(false)
}
