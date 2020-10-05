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

import alluxio.AlluxioURI
import alluxio.client.file.{FileInStream, FileOutStream, FileSystem}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.disagg.DisaggBlockManagerMessages._

private[spark] class DisaggBlockManager(
      var driverEndpoint: RpcEndpointRef,
      conf: SparkConf) extends Logging {

  val fs : FileSystem = FileSystem.Factory.get()

  def readLock(blockId: BlockId, executorId: String): Boolean = {
    val readSucceeded = driverEndpoint.askSync[Int](FileReadLock(blockId, executorId))
    if (readSucceeded == 0) {
      logInfo(s"Read lock $blockId: file not present in alluxio")
      false
    } else if (readSucceeded == 1) {
      logInfo(s"Read lock succeeded for $blockId " +
        s"executor $executorId ") // task ${TaskContext.get().taskAttemptId()}")
      true
    } else {
      // retry... the block is being written
      Thread.sleep(500)
      logInfo(s"Read lock try to acquire lock... $blockId " +
        s"executor $executorId ") // task ${TaskContext.get().taskAttemptId()}")
      readLock(blockId, executorId)
    }
  }

  def readUnlock(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask[Unit](FileReadUnlock(blockId, executorId))
    logInfo(s"Read unlock succeeded for $blockId " +
    s"executor $executorId ") // task ${TaskContext.get().taskAttemptId()}")
  }

  def getSize(blockId: BlockId, executorId: String): Long = {
    var size = -1L
    readLock(blockId, executorId)
    size = driverEndpoint.askSync[Long](GetSize(blockId))
    readUnlock(blockId, executorId)

    size
  }

  def writeLock(blockId: BlockId, executorId: String): Boolean = {
    val writeSucceeded = driverEndpoint.askSync[Boolean](FileWriteLock(blockId, executorId))
    if (writeSucceeded) {
      logInfo(s"Write lock succeeded for $blockId " +
        s"executor $executorId ") // task ${TaskContext.get().taskAttemptId()}")
      true
    } else {
      // retry...
      Thread.sleep(500)
      logInfo(s"Write lock try to acquire lock... $blockId " +
        s"executor $executorId ") //  task ${TaskContext.get().taskAttemptId()}")
      writeLock(blockId, executorId)
    }
  }

  def writeUnlock(blockId: BlockId, executorId: String, size: Long): Unit = {
    driverEndpoint.ask(FileWriteUnlock(blockId, size))
    logInfo(s"Write finished in alluxio, unlocked: $blockId " +
      s"executor $executorId ") // task ${TaskContext.get().taskAttemptId()}")
  }

  def createFileInputStream(blockId: BlockId, executorId: String): Option[FileInStream] = {
    val path = new AlluxioURI("/" + blockId)
    if (getSize(blockId, executorId) > 0L) {
      Some(fs.openFile(path))
    } else {
      None
    }
  }

  def createFileOutputStream(blockId: BlockId): FileOutStream = {
    val path = new AlluxioURI("/" + blockId)
    fs.createFile(path)
  }
}

private[spark] object DisaggBlockManager {
  val DRIVER_ENDPOINT_NAME = "DisaggBlockManager"
}

