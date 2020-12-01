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

package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, BlockManagerId}

private[spark] object SparkLogger extends Logging {
  def logSCT(stageId: Int, sct: String): Unit = {
    logInfo(s"Stage finished: $stageId, took $sct s")
  }

  def logJCT(jobId: String, jct: Double): Unit = {
    logInfo(s"Job finished: $jobId, took $jct s")
  }

  def logCacheMemory(blockId: BlockId, size: Long, blockManagerId: BlockManagerId): Unit = {
    logInfo(s"CACHE_M\t$blockId\t$size\t$blockManagerId")
  }

  def logCacheDisk(blockId: BlockId, size: Long, blockManagerId: BlockManagerId): Unit = {
    logInfo(s"CACHE_D\t$blockId\t$size\t$blockManagerId")
  }

  def logLocalMemHit(blockId: BlockId, blockManagerId: BlockManagerId): Unit = {
    logInfo(s"HIT_M\t$blockId\t$blockManagerId")
  }

  def logLocalDiskHit(blockId: BlockId, blockManagerId: BlockManagerId): Unit = {
    logInfo(s"HIT_D\t$blockId\t$blockManagerId")
  }

  def logRemoteMemHit(blockId: BlockId, blockManagerIds: Seq[BlockManagerId]): Unit = {
    logInfo(s"HIT_REMOTE_M\t$blockId\t${blockManagerIds.toList}")
  }

  def logRemoteDiskHit(blockId: BlockId, blockManagerIds: Seq[BlockManagerId]): Unit = {
    logInfo(s"HIT_REMOTE_D\t$blockId\t${blockManagerIds.toList}")
  }

  def logDiskRead(deserTime: Long, size: Long, blockId: BlockId,
                  blockManagerId: BlockManagerId): Unit = {
    logInfo(s"DiskIO: ReadLocal $blockId deserTime $deserTime size $size " +
      s"${blockManagerId.toString}")
  }

  def logDiskWrite(serTime: Long, size: Long, blockId: BlockId,
                   blockManagerId: BlockManagerId): Unit = {
    logInfo(s"DiskIO: Write $blockId serTime $serTime size $size " +
      s"${blockManagerId.toString}")
  }

  def logPromoteFail(blockId: BlockId, blockManagerId: BlockManagerId): Unit = {
    logInfo(s"DiskIO: PromoteFail $blockId ${blockManagerId.toString}")
  }

  def logPromote(blockId: BlockId, blockManagerId: BlockManagerId): Unit = {
    logInfo(s"DiskIO: Promote $blockId ${blockManagerId.toString}")
  }
}

