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

import org.apache.spark.storage._

/**
 * This policy does not cache the data into memory.
 */
class JoinDisaggStoringPolicy()
  extends DisaggStoringPolicy {

  override def isStoringEvictedBlockToDisagg(blockId: BlockId): Boolean = {
    blockId.name.startsWith("rdd_79_") || blockId.name.startsWith("rdd_74_") ||
      blockId.name.startsWith("rdd_75_") || blockId.name.startsWith("rdd_80_")
  }
}
