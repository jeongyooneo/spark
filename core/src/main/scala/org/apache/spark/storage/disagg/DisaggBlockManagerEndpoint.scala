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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReadWriteLock, StampedLock}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint}
import org.apache.spark.util.ThreadUtils

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}
import scala.concurrent.ExecutionContext

/**
 */
private[spark] abstract class DisaggBlockManagerEndpoint()
  extends CrailManager(true) with Logging with RpcEndpoint {

  // Public methods
  def removeExecutor(executorId: String): Unit

  def isRddCache(rddId: Int): Boolean

  def taskStarted(taskId: String): Unit

  def stageCompleted(stageId: Int): Unit
  def stageSubmitted(stageId: Int): Unit
  def removeFromLocal(blockId: BlockId, executorId: String): Unit

  class CrailBlockInfo(blockId: BlockId,
                       path: String) {
    val bid = blockId
    private var size: Long = 0L
    private var actualBlockSize: Long = 0L
    var read: Boolean = true
    var createdTime = System.currentTimeMillis()
    var refCnt: AtomicInteger = new AtomicInteger()
    var nectarCost: Long = 0L
    var writeDone = false

    override def toString: String = {
      s"<$bid/read:$read>"
    }

    def getSize: Long = {
      size
    }

    def getActualBlockSize: Long = {
      actualBlockSize
    }

    def setSize(s: Long): Unit = {
      size = s
      actualBlockSize = DisaggUtils.calculateDisaggBlockSize(size)
    }
  }
}




