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

import org.apache.spark.storage._
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.util.io.ChunkedByteBuffer

 /**
 * This policy does not cache the data into memory.
 */
class NoCachingPolicy(memoryStore: MemoryStore)
                  extends DisaggCachingPolicy(memoryStore) {

  override def maybeCacheDisaggValuesInMemory[T](
                 blockInfo: BlockInfo,
                 blockId: BlockId,
                 level: StorageLevel,
                 disaggIterator: Iterator[T]): Iterator[T] = {
    // do nothing
    disaggIterator
  }

  override def maybeCacheDisaggBytesInMemory(
                 blockInfo: BlockInfo,
                 blockId: BlockId,
                 level: StorageLevel,
                 disaggData: BlockData): Option[ChunkedByteBuffer] = {
    // do nothing
    None
  }

   override def shouldBlockStoredToDisagg(blockId: BlockId): Boolean = {
      true
   }
 }
