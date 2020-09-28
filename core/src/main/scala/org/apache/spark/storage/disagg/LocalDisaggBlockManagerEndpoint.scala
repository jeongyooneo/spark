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

/**
 */
private[spark] class LocalDisaggBlockManagerEndpoint(override val rpcEnv: RpcEnv,
                                          val isLocal: Boolean,
                                          conf: SparkConf,
                                          listenerBus: LiveListenerBus,
                                          blockManagerMaster: BlockManagerMasterEndpoint)
  extends DisaggBlockManagerEndpoint {

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  blockManagerMaster.setDisaggBlockManager(this)

  val executor: ExecutorService = Executors.newCachedThreadPool()
  val recentlyBlockCreatedTimeMap = new ConcurrentHashMap[BlockId, Long]()
  val localExecutorLockMap = new ConcurrentHashMap[String, Object]().asScala
  private val disaggBlockLockMap = new ConcurrentHashMap[BlockId, ReadWriteLock].asScala

  /**
   * Parameters for disaggregation caching.
   */

  // blocks stored in disagg
  val disaggBlockInfo: concurrent.Map[BlockId, CrailBlockInfo] =
    new ConcurrentHashMap[BlockId, CrailBlockInfo]().asScala

  private def fileRead(blockId: BlockId, executorId: String): Int = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      0
    } else {
      logInfo(s"file read disagg block $blockId at $executorId")
      1
    }
  }

  private def fileWrite(blockId: BlockId, executorId: String): Int = {
    val blockInfo = new CrailBlockInfo(blockId, executorId)
    disaggBlockInfo.put(blockId, blockInfo)
    1
  }

  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  // THIS METHOD SHOULD BE CALLED AFTER WRITE LOCK !!
  private def fileWriteEnd(blockId: BlockId, size: Long): Boolean = {
    val info = disaggBlockInfo.get(blockId)
    if (info.isEmpty) {
      logWarning(s"No disagg block for writing $blockId")
      throw new RuntimeException(s"no disagg block for writing $blockId")
    } else {
      val v = info.get
      v.setSize(size)
      v.createdTime = System.currentTimeMillis()
      v.writeDone = true
      logInfo(s"Storing file writing $blockId, size $size")
      true
    }
  }

  def contains(blockId: BlockId): Int = {
    val info = disaggBlockInfo.get(blockId)

    if (info.isEmpty) {
       // logInfo(s"disagg not containing $blockId")
      0
    } else {
      1
    }
  }

  private def blockWriteLock(blockId: BlockId): Unit = {
    logInfo(s"Hold writelock $blockId")
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    disaggBlockLockMap(blockId).writeLock().lock()
  }

  private def blockWriteUnlock(blockId: BlockId): Unit = {
    // logInfo(s"Release tryWritelock $blockId")
    disaggBlockLockMap(blockId).writeLock().unlock()
  }

  private def blockReadLock(blockId: BlockId, executorId: String): Boolean = {
    // logInfo(s"Hold readlock $blockId, $executorId")
    disaggBlockLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())
    disaggBlockLockMap(blockId).readLock().lock()
    true
  }

  private def blockReadUnlock(blockId: BlockId, executorId: String): Boolean = {
    // logInfo(s"Release readlock $blockId, $executorId")
    disaggBlockLockMap(blockId).readLock().unlock()
    true
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // FOR DISAGG
    // FOR DISAGG
    case FileWriteEnd(blockId, executorId, size) =>
      fileWriteEnd(blockId, size)
      blockWriteUnlock(blockId)

    case FileRead(blockId, executorId) =>
      if (blockReadLock(blockId, executorId)) {
        val result = fileRead(blockId, executorId)
        if (result == 0) {
          // logInfo(s"File unlock $blockId at $executorId for empty")
          blockReadUnlock(blockId, executorId)
        }
        context.reply(result)
      } else {
        context.reply(2)
      }

    case FileReadUnlock(blockId, executorId) =>
      // logInfo(s"File read unlock $blockId from $executorId")
      blockReadUnlock(blockId, executorId)

    case Contains(blockId, executorId) =>
      blockReadLock(blockId, executorId)
      try {
        val result = contains(blockId)
      } finally {
        blockReadUnlock(blockId, executorId)
      }
      context.reply(contains(blockId))

    case GetSize(blockId, executorId) =>
      blockReadLock(blockId, executorId)
      try {
        val size = if (disaggBlockInfo.get(blockId).isEmpty) {
          logWarning(s"disagg block is empty.. no size $blockId")
          0L
        } else {
          disaggBlockInfo.get(blockId).get.getSize
        }
        context.reply(size)
      } finally {
        blockReadUnlock(blockId, executorId)
      }

    case FileWrite(blockId, executorId) =>
      blockWriteLock(blockId)
      val result = fileWrite(blockId, executorId)
      context.reply(result)
  }
}
