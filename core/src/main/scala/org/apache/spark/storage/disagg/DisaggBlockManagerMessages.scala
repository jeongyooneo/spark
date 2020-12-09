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

import org.apache.spark.storage.BlockId

private[spark] object DisaggBlockManagerMessages {

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from slaves to the master.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  case class FileRemoved(blockId: BlockId, executorId: String, remove: Boolean)
    extends ToBlockManagerMaster

  case class FileRead(blockId: BlockId, executorId: String) extends ToBlockManagerMaster
  case class FileReadUnlock(blockId: BlockId, executorId: String) extends ToBlockManagerMaster

  case class DiscardBlocksIfNecessary(estimateSize: Long)
    extends ToBlockManagerMaster

  case class IsRddCache(rddId: Int) extends ToBlockManagerMaster

  case class StoreBlockOrNot(blockId: BlockId, estimateSize: Long, executorId: String,
                             putDisagg: Boolean, localFull: Boolean, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class CachingFail(blockId: BlockId, estimateSize: Long, executorId: String,
                             putDisagg: Boolean, localFull: Boolean, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class CachingDone(blockId: BlockId, estimateSize: Long, executorId: String, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class DiskCachingDone(blockId: BlockId, size: Long, executorId: String)
    extends ToBlockManagerMaster

  case class FileWriteEnd(blockId: BlockId, executorId: String, size: Long)
    extends ToBlockManagerMaster

  case class Contains(blockId: BlockId, executorId: String)
    extends ToBlockManagerMaster

  case class GetSize(blockId: BlockId, executorId: String)
    extends ToBlockManagerMaster

  // for local decision

  case class LocalEviction(blockId: Option[BlockId], executorId: String,
                           size: Long, prevEvicted: Set[BlockId], onDisk: Boolean)
    extends ToBlockManagerMaster

  case class EvictionFail(blockId: BlockId, executorId: String, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class LocalEvictionDone(blockId: BlockId, executorId: String, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class ReadBlockFromLocal(blockId: BlockId, executorId: String,
                                fromRemote: Boolean, onDisk: Boolean, readTime: Long)
  extends ToBlockManagerMaster

  // metric
  case class ReadDisaggBlock(blockId: BlockId, size: Long, time: Long) extends ToBlockManagerMaster
  case class WriteDisaggBlock(blockId: BlockId, size: Long, time: Long) extends ToBlockManagerMaster
  case class SendRecompTime(blockId: BlockId, time: Long) extends ToBlockManagerMaster
  case class SendNoCachedRDDCompTime(rddId: Int, time: Long) extends ToBlockManagerMaster
  case class SendRDDElapsedTime(srcBlock: String,
                                dstBlock: String,
                                clazz: String, time: Long) extends ToBlockManagerMaster

  case class PromoteToMemory(blockId: BlockId, size: Long,
                             executorId: String,
                             enoughSpace: Boolean,
                             fromDisk: Boolean) extends ToBlockManagerMaster

  case class GetLocalBlockSize(blockId: BlockId) extends ToBlockManagerMaster
}
