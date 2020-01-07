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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.reflect.ClassTag

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class DisaggStore(
    conf: SparkConf,
    blockManagerMaster: BlockManagerMaster,
    disaggManager: DisaggBlockManager,
    executorId: String)
  extends Logging {

  @transient lazy val mylogger = org.apache.log4j.LogManager.getLogger("myLogger")
  // private val blockSizes = new ConcurrentHashMap[BlockId, Long]()

  def getSize(blockId: BlockId): Long = {
    disaggManager.getSize(blockId)
  }

  /**
   * Invokes the provided callback function to write the specific block.
   *
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId, estimateSize: Long)(writeFunc: WritableByteChannel => Unit): Boolean = {
    // if (contains(blockId)) {
    //   throw new IllegalStateException(s"Block $blockId is already present in the disagg store")
    // }

    // first discard blocks from disagg memory
    // if the memory is full
    logInfo(s"discard block for storing $blockId if necessary in worker $estimateSize")

    if (!disaggManager.storeBlockOrNot(blockId, estimateSize)) {
      return false
    }

    try {
      val startTime = System.currentTimeMillis
      val file = disaggManager.createFile(blockId)

      if (file != null) {
        val out = new CountingWritableChannel(Channels.newChannel(
          file.getBufferedOutputStream(file.getCapacity)))
        var threwException: Boolean = true
        try {
          writeFunc(out)
          // blockSizes.put(blockId, out.getCount)
          logInfo(s"Attempting to put block $blockId  " +
            s"to disagg, size: ${out.getCount}, executor ${executorId}, " +
            s"blockSizes: ${out.getCount}")
          threwException = false
        } catch {
          case e: Exception =>
            e.printStackTrace()
            throw e
        } finally {
          try {
            out.close()
            disaggManager.writeEnd(blockId, out.getCount)
          } catch {
            case ioe: IOException =>
              if (!threwException) {
                threwException = true
                throw ioe
              }
          } finally {
            if (threwException) {
              remove(blockId)
            }
          }
        }

        val finishTime = System.currentTimeMillis
        logDebug("tg: Block %s stored as %s file on disagg in %d ms".format(
          blockId,
          file.getPath,
          finishTime - startTime))

        true
      } else {
        logInfo(s"File $blockId is already created ... so skip creating the file")
        false
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logWarning("Exception thrown when putting block " + blockId + ", " + e)
        throw e
    }
  }

  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer): Unit = {
    put(blockId, bytes.size) { channel =>
      bytes.writeFully(channel)
    }
  }

  def getStream(blockId: BlockId): CrailBlockData = {

    // disaggManager.read(blockId)

    val file = disaggManager.getFile(blockId)
    val blockSize = getSize(blockId)

    logInfo(s"jy: getMultiStream $executorId $blockId fs.lookup started, size $blockSize")

    if (blockSize <= 0) {
      throw new RuntimeException("Block size should be greater than 0 for getting bytes " + blockId)
    }

    new CrailBlockData(file.getBufferedInputStream(blockSize), blockSize)
  }

  def getBytes(blockId: BlockId): BlockData = {


    val file = disaggManager.getFile(blockId)
    val blockSize = getSize(blockId)

    logInfo(s"jy: getMultiStream $executorId $blockId fs.lookup started, size $blockSize")

    if (blockSize <= 0) {
      throw new RuntimeException("Block size should be greater than 0 for getting bytes " + blockId)
    }

    val disaggFetchStart = System.nanoTime
    val channel = Channels.newChannel(file.getBufferedInputStream(blockSize))
    Utils.tryWithSafeFinally {

      val buf = ByteBuffer.allocate(blockSize.toInt)
      JavaUtils.readFully(channel, buf)
      buf.flip()

      val disaggFetchTime = System.nanoTime - disaggFetchStart
      logInfo(s"jy: getMultiStream $executorId $blockId fs.lookup succeeded, $disaggFetchTime ns")

      new ByteBufferBlockData(new ChunkedByteBuffer(buf), true)
    } {
      channel.close()
    }
  }

  def remove(blockId: BlockId): Boolean = {
    disaggManager.remove(blockId)
  }

  def contains(blockId: BlockId): Boolean = {
    disaggManager.blockExists(blockId)
  }

  def readLock(blockId: BlockId): Boolean = {
    disaggManager.read(blockId)
  }

  def readUnlock(blockId: BlockId): Unit = {
    disaggManager.readUnlock(blockId)
  }
}
