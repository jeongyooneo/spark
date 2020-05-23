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
import org.apache.crail.utils.CrailUtils
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

private[spark] abstract class CrailManager(isDriver: Boolean,
                                           executorId: String) extends Logging {

  val prefix = "/spark-rdd-"

  // For disaggregated memory store
  val crailConf = new CrailConfiguration()
  var fs : CrailStore = _
  fs = CrailStore.newInstance(crailConf)

  var directory: String = _
  if (!isDriver) {
    directory = prefix + executorId
    logInfo("creating main dir " + directory)
    val baseDirExists: Boolean = fs.lookup(directory).get() != null
    if (baseDirExists) {
      fs.delete(directory, true).get().syncDir()
    }

    fs.create(directory, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailUtils.getLocationClass, true).get().syncDir()
    logInfo("creating main dir done " + directory)
  }

  def getPath(blockId: BlockId): String = {
    if (isDriver) {
      throw new RuntimeException("Please provide executorId")
    }
    return directory + "/" + blockId.name
  }

  def getPathForDriver(blockId: BlockId, executorId: String): String = {
    if (!isDriver) {
      throw new RuntimeException("Please set executor id")
    }
    return prefix + executorId + "/" + blockId.name
  }
}
