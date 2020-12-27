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
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

private[spark] object BlazeParameters extends Logging {

  val readThp = 3000.0 / (600 * 1024 * 1024)
  val writeThp = 3000.0 / (600 * 1024 * 1024)

  private[spark] val READ_THP = ConfigBuilder("spark.disagg.readthp")
    .doubleConf
    .createWithDefault(readThp)

  private[spark] val IS_SPARK = ConfigBuilder("spark.disagg.isspark")
    .booleanConf
    .createWithDefault(false)

  private[spark] val DISAGG_THRESHOLD_MB = ConfigBuilder("spark.disagg.threshold")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("0g")

    private[spark] val DISK_THRESHOLD = ConfigBuilder("spark.disagg.disk.threshold")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("0g")

  private[spark] val JOB_DAG_PATH = ConfigBuilder("spark.disagg.dagpath")
    .stringConf
    .createWithDefault("??")

  private[spark] val AUTOCACHING = ConfigBuilder("spark.disagg.autocaching")
    .booleanConf
    .createWithDefault(true)

  private[spark] val AUTOUNPERSIST = ConfigBuilder("spark.disagg.autounpersist")
    .booleanConf
    .createWithDefault(true)

  private[spark] val USE_DISK = ConfigBuilder("spark.disagg.useLocalDisk")
    .booleanConf
    .createWithDefault(false)

  private[spark] val DISK_LOCALITY_UNAWARE = ConfigBuilder("spark.disagg.diskLocalityUnaware")
    .booleanConf
    .createWithDefault(false)

  private[spark] val CACHING_POLICY = ConfigBuilder("spark.disagg.cachingpolicy")
    .stringConf
    .createWithDefault("Blaze")

  private[spark] val RAND_CACHING_POLICY_PARAM =
    ConfigBuilder("spark.disagg.cachingpolicy.random.percentage")
    .doubleConf
    .createWithDefault(0.2)

  private[spark] val SAMPLING = ConfigBuilder("spark.disagg.sampledRun")
    .booleanConf
    .createWithDefault(false)

  private[spark] val D_BANDWIDTH = ConfigBuilder("spark.disagg.diskBandwidth")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("500g")

  private[spark] val DISABLE_LOCAL_CACHING = ConfigBuilder("spark.disagg.disableLocalCaching")
    .booleanConf
    .createWithDefault(false)

  private[spark] val DISAGG_FIRST = ConfigBuilder("spark.disagg.first")
    .booleanConf
    .createWithDefault(false)

  private[spark] val COST_FUNCTION = ConfigBuilder("spark.disagg.costfunction")
    .stringConf
    .createWithDefault("No")

  private[spark] val EVICTION_POLICY = ConfigBuilder("spark.disagg.evictionpolicy")
    .stringConf
    .createWithDefault("Default")

  private[spark] val FULLY_PROFILED = ConfigBuilder("spark.disagg.fullyProfiled")
    .booleanConf
    .createWithDefault(true)

  // MB
  private[spark] val MEMORY_SLACK = ConfigBuilder("spark.disagg.memoryslack")
    .intConf
    .createWithDefault(300)

  private[spark] val PROMOTE_RATIO = ConfigBuilder("spark.disagg.promote")
    .doubleConf
    .createWithDefault(0.3)

  private[spark] val DISABLE_PROMOTE = ConfigBuilder("spark.disagg.promote.disable")
    .booleanConf
    .createWithDefault(false)

  private[spark] val ZIGZAG_RATIO = ConfigBuilder("spark.disagg.zigzag")
    .doubleConf
    .createWithDefault(0.5)

   private[spark] val CACHING_UNCONDITONALLY = ConfigBuilder("spark.disagg.cachingUnconditionally")
    .booleanConf
    .createWithDefault(false)
}
