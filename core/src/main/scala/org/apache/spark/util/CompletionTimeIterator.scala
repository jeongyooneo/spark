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

package org.apache.spark.util

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates
 * through all the elements.
 */
private[spark]
abstract class CompletionTimeIterator[ +A, +I <: Iterator[A]](sub: I) extends Iterator[A] {

  private[this] var accTime = 0L
  private[this] var completed = false
  private[this] var iter = sub
  private[this] var cnt = 0
  private[this] var prevTime = 0

  def next(): A = {
    val st = System.nanoTime()
    val v = iter.next()
    val et = System.nanoTime()
    accTime += (et - st)
    v
  }
  def hasNext: Boolean = {
    val r = iter.hasNext
    if (!r && !completed) {
      completed = true
      // reassign to release resources of highly resource consuming iterators early
      iter = Iterator.empty.asInstanceOf[I]
      completion(accTime)
    }
    r
  }

  def completion(accTime: Long): Unit
}


