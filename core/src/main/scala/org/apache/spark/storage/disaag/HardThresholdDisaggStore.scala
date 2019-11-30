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

import org.apache.spark.SparkConf
import org.apache.spark.memory.{MemoryMode, StaticMemoryManager}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockInfoManager, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.reflect.ClassTag


private[spark] class HardThresholdDisaggStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    executorId: String) extends DisaggStore (
  conf, blockInfoManager, serializerManager, executorId) {

  // create static memory manager
  // that manages the static portion of memory for caching
  val memoryManager = new StaticMemoryManager(conf, -1)

  /**
    * Acquire storage memory. If it cannot acquire,
    * it will call dropMemory method to drop evicted data to disagg.
    * If it return false, the block will be stored in disagg memory.
    *
    * @param blockId blockId for storing
    * @param size
    * @param memoryMode
    * @return
    */
  override def acquireStorageMemory(blockId: BlockId,
                                    size: Long,
                                    memoryMode: MemoryMode): Boolean = {
    // memory manager will automatically call dropFromMemory
    memoryManager.acquireStorageMemory(blockId, size, memoryMode)
  }

  /**
    * Drop a block from memory, possibly putting it on disk if applicable.
    * Called when the memory
    * store reaches its limit and needs to free up space.
    *
    * If `data` is not put on disk, it won't be created.
    *
    * The caller of this method must hold a write lock on the block before calling this method.
    * This method does not release the write lock.
    *
    * @return the block's new effective StorageLevel.
    */
  override def dropFromMemory[T: ClassTag]
  (blockId: BlockId, data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {

    StorageLevel.DISAGG
  }
}
