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
import alluxio.exception.status.UnavailableException

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
      logInfo(s"Read lock failed for $blockId: metadata and file not present in alluxio")
      false
    } else if (readSucceeded == 1) {
      logInfo(s"Read lock succeeded for $blockId " +
        s"executor $executorId")
      true
    } else {
      // retry... the block is being written
      Thread.sleep(500)
      logInfo(s"Read lock try to acquire lock... $blockId " +
        s"executor $executorId")
      readLock(blockId, executorId)
    }
  }

  def readUnlock(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask[Unit](FileReadUnlock(blockId, executorId))
    logInfo(s"Read unlock succeeded for $blockId " +
    s"executor $executorId")
  }

  /**
   * This method should be called only when read lock is held.
   * @param blockId of the block to remove metadata
   * @param executorId of the executor where the task that performs this is running
   */
  def removeFileInfo(blockId: BlockId, executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](RemoveFileInfo(blockId))
  }

  def getSize(blockId: BlockId, executorId: String): Long = {
    var size = -1L
    if (readLock(blockId, executorId)) {
      size = driverEndpoint.askSync[Long](GetSize(blockId))
      readUnlock(blockId, executorId)
    }

    size
  }

  def writeLock(blockId: BlockId, executorId: String): Unit = {
    val writeSucceeded = driverEndpoint.askSync[Boolean](FileWriteLock(blockId, executorId))
    if (writeSucceeded) {
      logInfo(s"Write lock succeeded for $blockId " +
        s"executor $executorId")
    } else {
      // retry...
      Thread.sleep(500)
      logInfo(s"Write lock: trying to acquire lock... $blockId " +
        s"executor $executorId")
      writeLock(blockId, executorId)
    }
  }

  def writeUnlock(blockId: BlockId, executorId: String, size: Long): Unit = {
    driverEndpoint.ask(FileWriteUnlock(blockId, executorId, size))
    logInfo(s"Write finished in alluxio, unlocked: $blockId " +
      s"executor $executorId")
  }

  def removeFile(blockId: BlockId): Boolean = {
    val path = new AlluxioURI("/" + blockId)
    try {
      if (fs.exists(path)) {
        // the block is evicted (and UnavailableException is thrown)
        // thus we removed metadata for the block,
        // but fs.exists(path) CAN return true, so we delete it
        fs.delete(path)
      }
      true
    } catch {
      case e: Exception => e.printStackTrace()
        logInfo(s"Inside removeFile: exception for $blockId")
        throw e
    }
  }

  def createFileInputStream(blockId: BlockId, executorId: String): Option[FileInStream] = {
    val path = new AlluxioURI("/" + blockId)
    logInfo(s"createFileInputStream for $blockId " +
      s"executor $executorId")
    try {
      // path exists and is not lost
      val exists = fs.exists(path)
      val isLost = fs.getStatus(path).getPersistenceState.contains("LOST")
      if (exists && !isLost) {
        logInfo(s"createFileInputStream for $blockId: " +
          s"executor $executorId")
        Some(fs.openFile(path))
      } else {
        logInfo(s"createFileInputStream for $blockId: path doesn't exist or evicted " +
          s"executor $executorId")
        None
      }
    } catch {
        case e: UnavailableException =>
          logInfo(s"inside createFileInputStream: $blockId not available (evicted) ")
          e.printStackTrace()
          None
        case e: Exception => e.printStackTrace()
          logInfo(s"Inside createFileInputStream: exception for $blockId ")
          throw e
    }
  }

  /**
   * Invariant: this method is called only when metadata for a block doesn't exist,
   * which means that either 1) the block is never written
   * or 2) the block is written but evicted (so Alluxio threw UnavailableException).
   * @param blockId block to put to Alluxio
   * @return the FileOutStream of the block
   */
  def createFileOutputStream(blockId: BlockId): FileOutStream = {
    val path = new AlluxioURI("/" + blockId)
    if (fs.exists(path)) {
      // the block is evicted (and UnavailableException is thrown)
      // thus we removed metadata for the block,
      // but fs.exists(path) CAN return true, so we delete it
      fs.delete(path)
    }
    fs.createFile(path)
    /*
    try {
      if (fs.exists(path)) {
        // the block is evicted (and UnavailableException is thrown)
        // thus we removed metadata for the block,
        // but fs.exists(path) CAN return true, so we delete it
        fs.delete(path)
      }
      fs.createFile(path)
    } catch {
      case e1: ResourceExhaustedException =>
        logInfo(s"ResourceExhaustedException for $blockId, falling back to GrpcDataWriter")
        // do nothing
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
     */
  }
}

private[spark] object DisaggBlockManager {
  val DRIVER_ENDPOINT_NAME = "DisaggBlockManager"
}



