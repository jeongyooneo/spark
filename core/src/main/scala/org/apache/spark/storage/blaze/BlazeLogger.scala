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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

private[spark] object BlazeLogger extends Logging {

  def logCachingDecision(rddId: Int): Unit = {
    logInfo(s"DECISION_CACHING\t$rddId")
  }

  def serTime(blockId: BlockId, size: Long, time: Long): Unit = {
    logInfo(s"SER\t$blockId\t$size\t$time")
  }

  def deserTime(blockId: BlockId, size: Long, time: Long): Unit = {
    logInfo(s"DESER\t$blockId\t$size\t$time")
  }

  def recompTime(blockId: BlockId, time: Long): Unit = {
    logInfo(s"RECOMP\t$blockId\t$time")
  }

  def rddCompTime(rddId: Int, time: Long): Unit = {
    logInfo(s"CMP\t$rddId\t$time")
  }

  def cachingDone(blockId: BlockId, stageId: String, size: Long, onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"CACHING_DONE_DISK\t$stageId\t$blockId\t$size")
    } else {
      logInfo(s"CACHING_DONE_MEM\t$stageId\t$blockId\t$size")
    }
  }

  def logLocalCaching(blockId: BlockId, executor: String,
                      size: Long,
                      comp: Double, diskCost: Long, msg: String,
                      onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"CACHING_DISK_L\t$executor\t$blockId\t$comp\t$diskCost\t$size\t$msg")
    } else {
      logInfo(s"CACHING_L\t$executor\t$blockId\t$comp\t$diskCost\t$size\t$msg")
    }
  }

  // local caching fail
  def cachingFailure(blockId: BlockId, executorId: String,
                     size: Long,
                     onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"FAIL_CACHING_DISK\t$executorId\t$blockId\t$size")
    } else {
      logInfo(s"FAIL_CACHING_L\t$executorId\t$blockId\t$size")
    }
  }

  // For executor failure
  def removeLocal(blockId: BlockId, executor: String,
                  size: Long): Unit = {
    logInfo(s"REMOVE_L\t$executor\t$blockId\t$size")
  }

  // Discard: this rdd is never cached
  def discardLocal(blockId: BlockId, executor: String,
                   reduction: Double,
                   diskCost: Long,
                   size: Long,
                   msg: String,
                   onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"DISCARD_DISK\t$executor\t$blockId\t$reduction\t$diskCost\t$size\t$msg")
    } else {
      logInfo(s"DISCARD_L\t$executor\t$blockId\t$reduction\t$diskCost\t$size\t$msg")
    }
  }

  def promote(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"PROMOTE\t$executorId\t$blockId")
  }

  def tryToPromote(blockId: BlockId, executorId: String, size: Long,
                   onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"TRY_PROMOTE_FROM_DISK\t$executorId\t$blockId\t$size")
    } else {
      logInfo(s"TRY_PROMOTE\t$executorId\t$blockId\t$size")
    }
  }

  // Evict: this rdd is cached
  def evictLocal(blockId: BlockId, executor: String,
                 comp: Double, diskCost: Long,
                 size: Long,
                 onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"EVICT_DISK\t$executor\t$blockId\t$comp\t$diskCost\t$size")
    } else {
      logInfo(s"EVICT_L\t$executor\t$blockId\t$comp\t$diskCost\t$size")
    }
  }

  // Read operations
  def readLocal(blockId: BlockId, stageId: String, fromRemote: Boolean,
                onDisk: Boolean, readTime: Long): Unit = {
    if (onDisk) {
      if (fromRemote) {
        logInfo(s"READ_DISK_R\t$stageId\t$blockId\t$fromRemote\t$readTime")
      } else {
        logInfo(s"READ_DISK_L\t$stageId\t$blockId\t$fromRemote\t$readTime")
      }
    } else {
      if (fromRemote) {
        logInfo(s"READ_L_R\t$stageId\t$blockId\t$fromRemote\t$readTime")
      } else {
        logInfo(s"READ_L_L\t$stageId\t$blockId\t$fromRemote\t$readTime")
      }
    }
  }

  def removeZeroBlocks(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"RM_ZERO_L\t$executorId\t$blockId")
  }

  def removeZeroBlocksDisk(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"RM_ZERO_DISK_L\t$executorId\t$blockId")
  }
}
