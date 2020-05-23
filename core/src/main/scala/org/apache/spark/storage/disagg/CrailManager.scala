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

import org.apache.crail._
import org.apache.crail.conf.CrailConfiguration
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

private[spark] abstract class CrailManager(isDriver: Boolean) extends Logging {

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

  if (isDriver) {
    logInfo("creating main dir " + rootDir)
    val baseDirExists: Boolean = fs.lookup(rootDir).get() != null
    if (baseDirExists) {
      fs.delete(rootDir, true).get().syncDir()
    }

    fs.create(rootDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + rootDir + " done")
    fs.create(broadcastDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + broadcastDir + " done")
    fs.create(shuffleDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + shuffleDir + " done")
    fs.create(rddDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + rddDir + " done")
    fs.create(tmpDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + tmpDir + " done")
    fs.create(metaDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + metaDir + " done")
    fs.create(hostsDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating main dir done " + rootDir)
  }

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
