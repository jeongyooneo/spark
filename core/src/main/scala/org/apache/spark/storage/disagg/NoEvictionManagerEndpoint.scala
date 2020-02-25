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
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}


/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class NoEvictionManagerEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    blockManagerMaster: BlockManagerMasterEndpoint,
    thresholdMB: Long)
  extends DisaggBlockManagerEndpoint(
    rpcEnv, isLocal, conf, listenerBus, blockManagerMaster, thresholdMB) {

  logInfo("LRUEvictionManagerEndpoint up")


  override def fileWriteEndCall(blockId: BlockId, size: Long): Unit = {

  }

  override def fileCreatedCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def fileReadCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def fileRemovedCall(blockInfo: CrailBlockInfo): Unit = {

  }

  override def taskStartedCall(taskId: String): Unit = {

  }

  override def stageCompletedCall(stageId: Int): Unit = {

  }

  override def stageSubmittedCall(stageId: Int): Unit = {

  }

  override def cachingDecision(blockId: BlockId, estimateSize: Long, taskId: String): Boolean = {
    true
  }
}
