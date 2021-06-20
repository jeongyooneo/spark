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

package org.apache.spark.storage.blaze

import org.apache.spark.storage.BlockId

private[spark] object BlazeBlockManagerMessages {

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from slaves to the master.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  case class IsRddToCache(rddId: Int) extends ToBlockManagerMaster

  case class StoreBlockOrNot(blockId: BlockId, estimateSize: Long, executorId: String,
                             localFull: Boolean, onDisk: Boolean, promote: Boolean,
                             taskAttempt: Long)
    extends ToBlockManagerMaster

  case class CachingFail(blockId: BlockId, estimateSize: Long, executorId: String,
                             localFull: Boolean, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class CachingDone(blockId: BlockId, estimateSize: Long, stageId: String, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class DiskCachingDone(blockId: BlockId, size: Long, executorId: String)
    extends ToBlockManagerMaster

  case class LocalEviction(blockId: Option[BlockId], executorId: String,
                           size: Long, prevEvicted: Set[BlockId], onDisk: Boolean)
    extends ToBlockManagerMaster

  case class SizePrediction(blockId: BlockId, executorId: String) extends ToBlockManagerMaster
  case class SendSize(blockId: BlockId, executorId: String, size: Long) extends ToBlockManagerMaster

  case class EvictionFail(blockId: BlockId, executorId: String, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class LocalEvictionDone(blockId: BlockId, executorId: String, onDisk: Boolean)
    extends ToBlockManagerMaster

  case class ReadBlockFromLocal(blockId: BlockId, stageId: String,
                                fromRemote: Boolean, onDisk: Boolean, readTime: Long)
  extends ToBlockManagerMaster

  case class TaskAttempBlockId(taskAttemp: Long, blockId: BlockId) extends ToBlockManagerMaster

  // metric
  case class SendSerTime(blockId: BlockId, size: Long, time: Long) extends ToBlockManagerMaster
  case class SendRecompTime(blockId: BlockId, time: Long) extends ToBlockManagerMaster
  case class SendCompTime(rddId: Int, time: Long) extends ToBlockManagerMaster
  case class SendRDDElapsedTime(srcBlock: String,
                                dstBlock: String,
                                clazz: String, time: Long) extends ToBlockManagerMaster

  case class PromoteToMemory(blockId: BlockId, size: Long,
                             executorId: String,
                             enoughSpace: Boolean,
                             fromDisk: Boolean) extends ToBlockManagerMaster

  case class GetLocalBlockSize(blockId: BlockId) extends ToBlockManagerMaster
}
