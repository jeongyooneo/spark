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

package org.apache.spark.sql.execution.datasources

import java.io.Closeable
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.SparkEnv
import org.apache.spark.input.WholeTextFileRecordReader
import org.apache.spark.internal.Logging
import org.apache.spark.storage.disagg.BlazeParameters

/**
 * An adaptor from a [[PartitionedFile]] to an [[Iterator]] of [[Text]], which is all of the lines
 * in that file.
 */
class HadoopFileWholeTextReader(file: PartitionedFile, conf: Configuration)
  extends Iterator[Text] with Closeable with Logging {

  private val sampledRun = SparkEnv.get.conf.get(BlazeParameters.SAMPLING)

  private val iterator = {
    val fileSplit = new CombineFileSplit(
      Array(new Path(new URI(file.filePath))),
      Array(file.start),
      Array(
        if (sampledRun) {
          1024
        } else {
          file.length
        }),
      // TODO: Implement Locality
      Array.empty[String])
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    val reader = new WholeTextFileRecordReader(fileSplit, hadoopAttemptContext, 0)
    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  private val CNT = 10
  private var read = 0

  override def hasNext: Boolean = {
    if (sampledRun) {
      if (read >= CNT) {
        logInfo(s"TG: Sampled run reading finished!!")
        false
      } else {
        iterator.hasNext
      }
    } else {
      iterator.hasNext
    }
  }

  override def next(): Text = {
    if (sampledRun) {
      read += 1
      iterator.next()
    } else {
      iterator.next()
    }
  }

  override def close(): Unit = iterator.close()
}
