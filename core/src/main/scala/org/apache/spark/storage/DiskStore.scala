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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.google.common.io.Closeables
import io.netty.channel.DefaultFileRegion
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.{AbstractFileRegion, JavaUtils}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage.disagg.{BlazeParameters, DisaggBlockManager, DisaggStore}
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.{SecurityManager, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Stores BlockManager blocks on disk.
 */
private[spark] class DiskStore(
    conf: SparkConf,
    diskManager: DiskBlockManager,
    securityManager: SecurityManager,
    blockManager: BlockManager,
    disaggManager: DisaggBlockManager,
    disaggStore: DisaggStore,
    executorId: String,
    memoryStore: MemoryStore) extends Logging {

  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")
  private val maxMemoryMapBytes = conf.get(config.MEMORY_MAP_LIMIT_FOR_TESTS)
  private val blockSizes = new ConcurrentHashMap[BlockId, Long]()

  private val THRESHOLD = conf.get(BlazeParameters.DISK_THRESHOLD)
  private val totalSize = new AtomicLong(0)
  private val pendingSize = new AtomicLong(0)

  def getSize(blockId: BlockId): Long = blockSizes.get(blockId)

  /**
   * Invokes the provided callback function to write the specific block.
   *
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: WritableByteChannel => Unit): Unit = {
    if (contains(blockId)) {
      logWarning(s"Block $blockId is already present in the disk store")
      return
    }

    logDebug(s"Attempting to put block $blockId")
    logInfo(s"Disk threshold: $THRESHOLD, totalSize: $totalSize")

    if (!blockId.isRDD) {
      val startTime = System.currentTimeMillis
      val file = diskManager.getFile(blockId)
      val out = new CountingWritableChannel(openForWrite(file))
      var threwException: Boolean = true
      try {
        writeFunc(out)
        blockSizes.put(blockId, out.getCount)
        totalSize.addAndGet(out.getCount)
        threwException = false
      } finally {
        try {
          out.close()
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
      logInfo("Block %s stored as %s file on disk in %d ms".format(
        file.getName,
        Utils.bytesToString(file.length()),
        finishTime - startTime))

      return
    }


    val size = memoryStore.getEstimateSize(blockId)
    var total = 0L
    var requiredEvictionSize = 0L

    val pending = pendingSize.synchronized {
      total = totalSize.get()
      pendingSize.addAndGet(size)
    }

    if (total + pending > THRESHOLD) {
      requiredEvictionSize = size
    }

    if (!disaggManager.cachingDecision(blockId, size,
      executorId, false, requiredEvictionSize > 0L, true)) {
      pendingSize.addAndGet(-size)
      return
    }

    // We should evict if requiredEvictionSize > 0
    val prevEvictedSelection = new mutable.HashSet[BlockId]()
    var cnt = 0
    while (totalSize.get() + pending > THRESHOLD && cnt < 2) {
      val evictBlockList: List[BlockId] =
        disaggManager.localEviction(
          Option(blockId), executorId, size, prevEvictedSelection.toSet, true)

      evictBlockList.foreach {
        eblock => prevEvictedSelection.add(eblock)
      }

      val iterator = evictBlockList.iterator

      logInfo(s"LocalDecision] Trying to evict blocks $evictBlockList " +
        s"from executor disk $executorId, totalSize: ${totalSize.get()}")

      while (iterator.hasNext) {
        val bid = iterator.next()
        val bsize = blockSizes.get(bid)
        logInfo(s"Evicting block $bid from disk size $bsize")
        if (blockManager.removeBlockFromDisk(bid, true)) {
          logInfo(s"Evicting done $bid")
          disaggManager.localEvictionDone(blockId, executorId, true)
        } else {
          disaggManager.evictionFail(bid, executorId, true)
        }
      }
      cnt += 1
    }

    if (totalSize.get() + pending > THRESHOLD) {
      disaggManager.cachingFail(blockId, size, executorId, false, true, true)
      return
    }

    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val out = new CountingWritableChannel(openForWrite(file))
    var threwException: Boolean = true
    try {
      writeFunc(out)
      blockSizes.put(blockId, out.getCount)

      pendingSize.synchronized {
        pendingSize.addAndGet(-size)
        totalSize.addAndGet(out.getCount)
      }

      threwException = false
    } finally {
      try {
        out.close()
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

    disaggManager.diskCachingDone(blockId, out.getCount, executorId)

    logInfo("Block %s stored as %s file on disk in %d ms".format(
      file.getName,
      Utils.bytesToString(file.length()),
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { channel =>
      bytes.writeFully(channel)
    }
  }

  def getBytes(blockId: BlockId): BlockData = {
    val file = diskManager.getFile(blockId.name)
    val blockSize = getSize(blockId)

    securityManager.getIOEncryptionKey() match {
      case Some(key) =>
        // Encrypted blocks cannot be memory mapped; return a special object that does decryption
        // and provides InputStream / FileRegion implementations for reading the data.
        new EncryptedBlockData(file, blockSize, conf, key)

      case _ =>
        new DiskBlockData(minMemoryMapBytes, maxMemoryMapBytes, file, blockSize)
    }
  }

  def remove(blockId: BlockId, toDisagg: Boolean = false): Boolean = {
    val bsize = blockSizes.get(blockId)
    blockSizes.remove(blockId)
    totalSize.addAndGet(-bsize)
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      if (blockId.isRDD && toDisagg) {
        // Send to disagg or not?
        if (disaggManager
          .cachingDecision(blockId, bsize, executorId, true, true, true)) {
          val inputStream = new FileInputStream(file)
          logInfo(s"Caching from disk to disagg $blockId, executor $executorId")
          disaggStore.put(blockId,
            bsize,
            executorId) { channel =>
            val out = Channels.newOutputStream(channel)
            IOUtils.copy(inputStream, out)
            inputStream.close()
          }
        }

        val ret = file.delete()
        if (!ret) {
          logWarning(s"Error deleting ${file.getPath()}")
        }
        ret
      } else {
        val ret = file.delete()
        if (!ret) {
          logWarning(s"Error deleting ${file.getPath()}")
        }
      ret
      }
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }

  private def openForWrite(file: File): WritableByteChannel = {
    val out = new FileOutputStream(file).getChannel()
    try {
      securityManager.getIOEncryptionKey().map { key =>
        CryptoStreamUtils.createWritableChannel(out, conf, key)
      }.getOrElse(out)
    } catch {
      case e: Exception =>
        Closeables.close(out, true)
        file.delete()
        throw e
    }
  }

}

private class DiskBlockData(
    minMemoryMapBytes: Long,
    maxMemoryMapBytes: Long,
    file: File,
    blockSize: Long) extends BlockData {

  override def toInputStream(): InputStream = new FileInputStream(file)

  /**
  * Returns a Netty-friendly wrapper for the block's data.
  *
  * Please see `ManagedBuffer.convertToNetty()` for more details.
  */
  override def toNetty(): AnyRef = new DefaultFileRegion(file, 0, size)

  override def toChunkedByteBuffer(allocator: (Int) => ByteBuffer): ChunkedByteBuffer = {
    Utils.tryWithResource(open()) { channel =>
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        val chunkSize = math.min(remaining, maxMemoryMapBytes)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(channel, chunk)
        chunk.flip()
        chunks += chunk
      }
      new ChunkedByteBuffer(chunks.toArray)
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    require(blockSize < maxMemoryMapBytes,
      s"can't create a byte buffer of size $blockSize" +
      s" since it exceeds ${Utils.bytesToString(maxMemoryMapBytes)}.")
    Utils.tryWithResource(open()) { channel =>
      if (blockSize < minMemoryMapBytes) {
        // For small files, directly read rather than memory map.
        val buf = ByteBuffer.allocate(blockSize.toInt)
        JavaUtils.readFully(channel, buf)
        buf.flip()
        buf
      } else {
        channel.map(MapMode.READ_ONLY, 0, file.length)
      }
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = {}

  private def open() = new FileInputStream(file).getChannel
}

private[spark] class EncryptedBlockData(
    file: File,
    blockSize: Long,
    conf: SparkConf,
    key: Array[Byte]) extends BlockData {

  override def toInputStream(): InputStream = Channels.newInputStream(open())

  override def toNetty(): Object = new ReadableChannelFileRegion(open(), blockSize)

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val source = open()
    try {
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        val chunkSize = math.min(remaining, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(source, chunk)
        chunk.flip()
        chunks += chunk
      }

      new ChunkedByteBuffer(chunks.toArray)
    } finally {
      source.close()
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    // This is used by the block transfer service to replicate blocks. The upload code reads
    // all bytes into memory to send the block to the remote executor, so it's ok to do this
    // as long as the block fits in a Java array.
    assert(blockSize <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
      "Block is too large to be wrapped in a byte buffer.")
    val dst = ByteBuffer.allocate(blockSize.toInt)
    val in = open()
    try {
      JavaUtils.readFully(in, dst)
      dst.flip()
      dst
    } finally {
      Closeables.close(in, true)
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = { }

  private def open(): ReadableByteChannel = {
    val channel = new FileInputStream(file).getChannel()
    try {
      CryptoStreamUtils.createReadableChannel(channel, conf, key)
    } catch {
      case e: Exception =>
        Closeables.close(channel, true)
        throw e
    }
  }
}

private[spark] class EncryptedManagedBuffer(
    val blockData: EncryptedBlockData) extends ManagedBuffer {

  // This is the size of the decrypted data
  override def size(): Long = blockData.size

  override def nioByteBuffer(): ByteBuffer = blockData.toByteBuffer()

  override def convertToNetty(): AnyRef = blockData.toNetty()

  override def createInputStream(): InputStream = blockData.toInputStream()

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this
}

private class ReadableChannelFileRegion(source: ReadableByteChannel, blockSize: Long)
  extends AbstractFileRegion {

  private var _transferred = 0L

  private val buffer = ByteBuffer.allocateDirect(64 * 1024)
  buffer.flip()

  override def count(): Long = blockSize

  override def position(): Long = 0

  override def transferred(): Long = _transferred

  override def transferTo(target: WritableByteChannel, pos: Long): Long = {
    assert(pos == transferred(), "Invalid position.")

    var written = 0L
    var lastWrite = -1L
    while (lastWrite != 0) {
      if (!buffer.hasRemaining()) {
        buffer.clear()
        source.read(buffer)
        buffer.flip()
      }
      if (buffer.hasRemaining()) {
        lastWrite = target.write(buffer)
        written += lastWrite
      } else {
        lastWrite = 0
      }
    }

    _transferred += written
    written
  }

  override def deallocate(): Unit = source.close()
}

private class CountingWritableChannel(sink: WritableByteChannel) extends WritableByteChannel {

  private var count = 0L

  def getCount: Long = count

  override def write(src: ByteBuffer): Int = {
    val written = sink.write(src)
    if (written > 0) {
      count += written
    }
    written
  }

  override def isOpen(): Boolean = sink.isOpen()

  override def close(): Unit = sink.close()

}
