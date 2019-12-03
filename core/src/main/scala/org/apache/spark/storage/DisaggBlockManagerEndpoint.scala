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

package org.apache.spark.storage

import org.apache.crail.{CrailLocationClass, CrailNodeType, CrailStorageClass}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.disaag.CrailManager
import org.apache.spark.storage.disaag.DisaggBlockManagerMessages._
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class DisaggBlockManagerEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging with CrailManager {

  logInfo("creating main dir " + rootDir)
  val baseDirExists : Boolean = fs.lookup(rootDir).get() != null

  logInfo("creating main dir " + rootDir)
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

  // Mapping from block manager id to the block manager's information.

  // disagg block size info
  private val disaggBlockInfo = new mutable.HashMap[BlockId, CrailBlockInfo]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  logInfo("DisaggBlockManagerEndpoint up")

  def fileCreated(blockId: BlockId): Boolean = {
    logInfo(s"Disagg endpoint: file created: $blockId")
    if (disaggBlockInfo.contains(blockId)) {
      logInfo(s"tg: Disagg block is already created $blockId")
      false
    } else {
      disaggBlockInfo(blockId) = new CrailBlockInfo(getPath(blockId))
      true
    }
  }

  def fileRemoved(blockId: BlockId): Boolean = {
    logInfo(s"Disagg endpoint: file removed: $blockId")
    disaggBlockInfo.remove(blockId).isDefined
  }

  def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    logInfo(s"Disagg endpoint: file write end: $blockId, size $size")
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
      logWarning(s"No disagg block for writing $blockId")
      false
    } else {
      info.synchronized {
        info.get.size = size
        info.get.writeDone = true
        info.get.notifyAll()
        logInfo(s"End of disagg file writing $blockId")
      }
      true
    }
  }

  def contains(blockId: BlockId): Boolean = {
    val info = disaggBlockInfo.get(blockId)
    logInfo(s"Disagg endpoint: contains: $blockId")

    if (info.isEmpty) {
      logInfo(s"disagg not containing $blockId")
      false
    } else {
      info.synchronized {
        while (!info.get.writeDone) {
          logInfo(s"Waiting for disagg block writing $blockId")
          info.get.wait()
        }
      }
      true
    }

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case FileCreated(blockId) =>
      context.reply(fileCreated(blockId))

    case FileRemoved(blockId) =>
      context.reply(fileRemoved(blockId))

    case FileWriteEnd(blockId, size) =>
      context.reply(fileWriteEnd(blockId, size))

    case Contains(blockId) =>
      context.reply(contains(blockId))

    case GetSize(blockId) =>
      context.reply(disaggBlockInfo.get(blockId).size)

  }
}

private class CrailBlockInfo(path: String) {
  var writeDone: Boolean = false
  var size: Long = 0L
}
