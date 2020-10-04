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

import java.util.concurrent.{ConcurrentHashMap, Executors, ExecutorService}
import java.util.concurrent.locks.{ReadWriteLock, StampedLock}

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.concurrent.ExecutionContext

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._
import org.apache.spark.util.ThreadUtils

private[spark] class LocalDisaggBlockManagerEndpoint(override val rpcEnv: RpcEnv,
                                          val isLocal: Boolean,
                                          conf: SparkConf,
                                          listenerBus: LiveListenerBus,
                                          blockManagerMaster: BlockManagerMasterEndpoint)
  extends DisaggBlockManagerEndpoint {

  logInfo(s"LocalDisaggBlockManagerEndpoint is up")

  blockManagerMaster.setDisaggBlockManager(this)
  if (blockManagerMaster.isLocal) {
    logInfo(s"BlockManagerMaster is local")
  }

  private val disaggBlockLockMap = new ConcurrentHashMap[BlockId, ReadWriteLock].asScala

  // blocks stored in disagg
  val disaggBlockInfo: concurrent.Map[BlockId, CrailBlockInfo] =
    new ConcurrentHashMap[BlockId, CrailBlockInfo]().asScala

  private def fileInfoExists(blockId: BlockId, executorId: String): Int = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      0
    } else {
      1
    }
  }

  private def fileWrite(blockId: BlockId, executorId: String): Boolean = {
    val blockInfo = new CrailBlockInfo(blockId, executorId)
    disaggBlockInfo.put(blockId, blockInfo)
    true
  }

  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  private def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      throw new RuntimeException(s"Written file for a non-existent block $blockId")
    } else {
      val v = info.get
      v.setSize(size)
      v.createdTime = System.currentTimeMillis()
      v.writeDone = true
      logInfo(s"file write to alluxio ended $blockId, size $size")
      true
    }
  }

  private def blockReadLock(blockId: BlockId, executorId: String): Boolean = {
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    if (disaggBlockLockMap(blockId).readLock().tryLock()) {
      logInfo(s"Held readlock $blockId, executor $executorId")
      true
    } else {
      logInfo(s"Failed to hold readlock $blockId, executor $executorId")
      false
    }
  }

  private def blockReadUnlock(blockId: BlockId, executorId: String): Boolean = {
    disaggBlockLockMap(blockId).readLock().unlock()
    logInfo(s"Released readlock $blockId, executor $executorId")
    true
  }

  private def blockWriteLock(blockId: BlockId): Boolean = {
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    if (disaggBlockLockMap(blockId).writeLock().tryLock()) {
      logInfo(s"Held writelock $blockId")
      true
    } else {
      false
    }
  }

  private def blockWriteUnlock(blockId: BlockId): Unit = {
    disaggBlockLockMap(blockId).writeLock().unlock()
    logInfo(s"Released writelock $blockId")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case FileReadLock(blockId, executorId) =>
      if (blockReadLock(blockId, executorId)) {
        // held read lock
        context.reply(fileInfoExists(blockId, executorId))
      } else {
        context.reply(2)
      }

    case FileReadUnlock(blockId, executorId) =>
      // logInfo(s"File read unlock $blockId from $executorId")
      blockReadUnlock(blockId, executorId)

    case FileWriteLock(blockId, executorId) =>
      if (blockWriteLock(blockId)) {
        val result = fileWrite(blockId, executorId)
        context.reply(result)
      } else {
        context.reply(false)
      }

    case FileWriteUnlock(blockId, size) =>
      fileWriteEnd(blockId, size)
      blockWriteUnlock(blockId)

    case GetSize(blockId) =>
      val size = if (disaggBlockInfo.get(blockId).isEmpty) {
        logWarning(s"disagg block is empty.. no size $blockId")
        -1L
      } else {
        disaggBlockInfo.get(blockId).get.getSize
      }
      context.reply(size)
  }
}

