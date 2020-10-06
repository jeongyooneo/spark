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

  case class FileReadLock(blockId: BlockId, executorId: String) extends ToBlockManagerMaster

  case class FileReadUnlock(blockId: BlockId, executorId: String) extends ToBlockManagerMaster

  case class FileWriteLock(blockId: BlockId, executorId: String)
    extends ToBlockManagerMaster

  case class FileWriteUnlock(blockId: BlockId, executorId: String, size: Long)
    extends ToBlockManagerMaster

  case class RemoveFileInfo(blockId: BlockId)
    extends ToBlockManagerMaster

  case class GetSize(blockId: BlockId)
    extends ToBlockManagerMaster

  // metric
  case class ReadDisaggBlock(blockId: BlockId, time: Long) extends ToBlockManagerMaster
  case class WriteDisaggBlock(blockId: BlockId, time: Long) extends ToBlockManagerMaster
  case class SendRecompTime(blockId: BlockId, time: Long) extends ToBlockManagerMaster
  case class SendNoCachedRDDCompTime(rddId: Int, time: Long) extends ToBlockManagerMaster

  case class CacheDisaggInMemory(blockId: BlockId, size: Long,
                                 executorId: String,
                                 enoughSpace: Boolean) extends ToBlockManagerMaster
}
