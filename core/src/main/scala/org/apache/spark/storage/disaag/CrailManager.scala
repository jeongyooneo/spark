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

import org.apache.crail._
import org.apache.crail.conf.CrailConfiguration
import org.apache.spark.storage.BlockId

private[spark] trait CrailManager {

  val rootDir = "/spark"
  val broadcastDir = rootDir + "/broadcast"
  val shuffleDir = rootDir + "/shuffle"
  val rddDir = rootDir + "/rdd"
  val tmpDir = rootDir + "/tmp"
  val metaDir = rootDir + "/meta"
  val hostsDir = metaDir + "/hosts"

  // For disaggregated memory store
  val crailConf = new CrailConfiguration()
  var fs : CrailStore = _
  fs = CrailStore.newInstance(crailConf)


  def getPath(blockId: BlockId): String = {
    var name = tmpDir + "/" + blockId.name
    if (blockId.isBroadcast) {
      name = broadcastDir + "/" + blockId.name
    } else if (blockId.isShuffle) {
      name = shuffleDir + "/" + blockId.name
    } else if (blockId.isRDD) {
      name = rddDir + "/" + blockId.name
    }
    return name
  }
}
