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

  def logLocalCaching(blockId: BlockId, executor: String,
                      size: Long,
                      comp: Double, disaggCost: Long, msg: String): Unit = {
    logInfo(s"CACHING_L\t$executor\t$blockId\t$size\t$msg")
  }

  def logLocalCachingDone(blockId: BlockId, executor: String,
                      size: Long, msg: String): Unit = {
    logInfo(s"SUCCESS_CACHING_L\t$executor\t$blockId\t$size\t$msg")
  }

  def logLocalDiskCaching(blockId: BlockId, executor: String,
                          size: Long,
                          comp: Double, disaggCost: Long, msg: String): Unit = {
    logInfo(s"CACHING_DISK_L\t$executor\t$blockId\t$size\t$msg")
  }

  def logDisaggCaching(blockId: BlockId,
                       size: Long,
                       comp: Double, disaggCost: Long): Unit = {
    logInfo(s"CACHING_D\t$blockId\t$size\t$comp\t$disaggCost\t$size")
  }

  // local caching fail
  def cachingFailure(blockId: BlockId, executorId: String,
                     size: Long): Unit = {
    logInfo(s"FAIL_CACHING_L\t$executorId\t$blockId\t$size")
  }

  // For executor failure
  def removeLocal(blockId: BlockId, executor: String,
                  size: Long): Unit = {
    logInfo(s"REMOVE_L\t$executor\t$blockId\t$size")
  }

  // Discard: this rdd is never cached in local/disagg
  def discardLocal(blockId: BlockId, executor: String,
                   reduction: Double,
                   disaggCost: Long,
                   size: Long,
                   msg: String): Unit = {
    logInfo(s"DISCARD_L\t$executor\t$blockId\t$reduction\t$disaggCost\t$size\t$msg")
  }
  def discardDisagg(blockId: BlockId, reduction: Double,
                    disagg: Long, size: Long, msg: String): Unit = {
    logInfo(s"DISCARD_D\t$blockId\t$reduction\t$disagg\t$size\t$msg")
  }

  def recacheDisaggToLocal(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"PROMOTE\t$executorId\t$blockId")
  }

  // Evict: this rdd is cached in local/disagg
  def evictLocal(blockId: BlockId, executor: String,
                 comp: Double, disaggCost: Long,
                 size: Long): Unit = {
    logInfo(s"EVICT_L\t$executor\t$blockId\t$comp\t$disaggCost\t$size")
  }
  def evictDisk(blockId: BlockId, executor: String,
                size: Long): Unit = {
    logInfo(s"EVICT_DISK_L\t$executor\t$blockId\t$size")
  }
  def evictDisagg(blockId: BlockId,
                  comp: Double,
                  disagg: Long,
                  size: Long): Unit = {
    logInfo(s"EVICT_D\t$blockId\t$comp\t$disagg\t$size")
  }

  def evictDisaggByLocal(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"EVICT_BY_LOCAL_D\t$blockId\t$executorId")
  }

  // Read operations
  def readLocal(blockId: BlockId, executorId: String, fromRemote: Boolean): Unit = {
    logInfo(s"READ_L\t$executorId\t$blockId\t$fromRemote")
  }

  def removeZeroBlocks(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"RM_ZERO_L\t$executorId\t$blockId")
  }

  def removeDuplicateBlocksInDisagg(blockId: BlockId): Unit = {
    logInfo(s"RM_DUP_D\t$blockId")
  }

  def removeZeroBlocksInDisagg(blockId: BlockId): Unit = {
    logInfo(s"RM_ZERO_D\t$blockId")
  }

  def readDisagg(blockId: BlockId): Unit = {
    logInfo(s"READ_D\t$blockId")
  }
}
