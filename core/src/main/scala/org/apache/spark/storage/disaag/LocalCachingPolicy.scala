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

import java.nio.ByteBuffer

import org.apache.spark.memory.MemoryMode
import org.apache.spark.storage._
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.reflect.ClassTag

/**
* This policy does not cache the data into memory.
*/
class LocalCachingPolicy(memoryStore: MemoryStore,
                         blockInfoManager: BlockInfoManager,
                         disaggStore: DisaggStore)
                 extends DisaggCachingPolicy(memoryStore) {

  override def maybeCacheDisaggValuesInMemory[T](
                                                  blockInfo: BlockInfo,
                                                  blockId: BlockId,
                                                  level: StorageLevel,
                                                  disaggIterator: Iterator[T]): Iterator[T] = {
    // cache the local block
    blockInfo.synchronized {
      if (memoryStore.contains(blockId)) {
        // Note: if we had a means to discard the disk iterator, we would do that here.
        memoryStore.getValues(blockId).get
      } else {
        logInfo("tg: cache block $blockId into memory from disagg")
        val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
        memoryStore.putIteratorAsValues(blockId, disaggIterator, classTag) match {
          case Left(iter) =>
            // The memory store put() failed, so it returned the iterator back to us:
            iter
          case Right(_) =>
            // The put() succeeded, so we can read the values back:
            logInfo(s"Removing disagg after caching value $blockId")
            disaggStore.remove(blockId)
            memoryStore.getValues(blockId).get
            // remove the value after storing it in memory
        }
      }
    }.asInstanceOf[Iterator[T]]
  }

 override def maybeCacheDisaggBytesInMemory(
                blockInfo: BlockInfo,
                blockId: BlockId,
                level: StorageLevel,
                disaggData: BlockData): Option[ChunkedByteBuffer] = {
   require(!level.deserialized)
   if (level.useMemory) {
     // Synchronize on blockInfo to guard against a race condition where two readers both try to
     // put values read from disagg into the MemoryStore.
     blockInfo.synchronized {
       if (memoryStore.contains(blockId)) {
         disaggData.dispose()
         Some(memoryStore.getBytes(blockId).get)
       } else {
         val allocator = level.memoryMode match {
           case MemoryMode.ON_HEAP => ByteBuffer.allocate _
           case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
         }
         val putSucceeded = memoryStore.putBytes(blockId, disaggData.size, level.memoryMode, () => {
           // https://issues.apache.org/jira/browse/SPARK-6076
           // If the file size is bigger than the free memory, OOM will happen. So if we
           // cannot put it into MemoryStore, copyForMemory should not be created. That's why
           // this action is put into a `() => ChunkedByteBuffer` and created lazily.
           disaggData.toChunkedByteBuffer(allocator)
         })
         if (putSucceeded) {
           disaggData.dispose()
           logInfo(s"Removing disagg after caching value $blockId")
           disaggStore.remove(blockId)
           Some(memoryStore.getBytes(blockId).get)
         } else {
           None
         }
       }
     }
   } else {
     None
   }
 }
}
