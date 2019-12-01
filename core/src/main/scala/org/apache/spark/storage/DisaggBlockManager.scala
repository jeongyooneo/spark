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

package org.apache.spark.storage

import java.util.concurrent.ConcurrentHashMap

import org.apache.crail.conf.CrailConfiguration
import org.apache.crail._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.disaag.{CrailBlockFile, NoCachingPolicy}
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.util.io.ChunkedByteBuffer

private[spark] class DisaggBlockManager(
      conf: SparkConf,
      executorId: String) extends Logging {

    // For disaggregated memory store
  val crailConf = CrailConfiguration.createEmptyConfiguration()
  var fs : CrailStore = _
  fs = CrailStore.newInstance(crailConf)
  var fileCache : ConcurrentHashMap[String, CrailBlockFile] = _
  fileCache = new ConcurrentHashMap[String, CrailBlockFile]()

  val rootDir = "/spark"
  val broadcastDir = rootDir + "/broadcast"
  val shuffleDir = rootDir + "/shuffle"
  val rddDir = rootDir + "/rdd"
  val tmpDir = rootDir + "/tmp"
  val metaDir = rootDir + "/meta"
  val hostsDir = metaDir + "/hosts"

  if (executorId == "driver") {
    logInfo("creating main dir " + rootDir)
    val baseDirExists : Boolean = fs.lookup(rootDir).get() != null

    logInfo("creating main dir " + rootDir)
    if (baseDirExists) {
      fs.delete(rootDir, true).get().syncDir()
    }
    fs.create(rootDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + rootDir + " done")
    fs.create(broadcastDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + broadcastDir + " done")
    fs.create(shuffleDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + shuffleDir + " done")
    fs.create(rddDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + rddDir + " done")
    fs.create(tmpDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + tmpDir + " done")
    fs.create(metaDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating " + metaDir + " done")
    fs.create(hostsDir, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().syncDir()
    logInfo("creating main dir done " + rootDir)
  }

  def createFile(blockId: BlockId) : CrailFile = synchronized {

    logInfo("jy: disagg: fresh file, writing " + blockId.name)
    val path = getPath(blockId)
    val fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT,
      CrailLocationClass.DEFAULT, true).get().asFile()
    fileInfo

    /*
    var crailFile = fileCache.get(blockId.name)
    if (crailFile == null) {
      crailFile = new CrailBlockFile(blockId.name, null)
      val oldFile = fileCache.putIfAbsent(blockId.name, crailFile)
      if (oldFile != null) {
        crailFile = oldFile
      }
    }

    fs.

    // create actual file
    crailFile.synchronized {
      if (crailFile.getFile() == null) {
        logInfo("jy: disagg: fresh file, writing " + blockId.name)
        val path = getPath(blockId)
        try {
          val fileInfo = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT,
            CrailLocationClass.DEFAULT, true).get().asFile()
          crailFile.update(fileInfo)
        }
      }
    }

    crailFile
    */
  }

  def getFile(blockId: BlockId): CrailFile = {
    val path = getPath(blockId)
    fs.lookup(path).get().asFile()

    /*
    var crailFile = fileCache.get(blockId.name)
    if (crailFile == null) {
      crailFile = new CrailBlockFile(blockId.name, null)
      val oldFile = fileCache.putIfAbsent(blockId.name, crailFile)
      if (oldFile != null) {
        crailFile = oldFile
      }
    }
    crailFile
    */
  }

  def remove(blockId: BlockId): Boolean = {
    if (blockExists(blockId)) {
      val path = getPath(blockId)
      fs.delete(path, false).get().syncDir()
      logInfo(s"jy: Removed block $blockId from disagg")
      true
    } else {
      false
    }
  }

  def getPath(blockId: BlockId): String = {
    var name = tmpDir + "/" + blockId.name
    if (blockId.isBroadcast) {
      name = broadcastDir + "/" + blockId.name
    } else if (blockId.isShuffle) {
      name = shuffleDir + "/" + blockId.name
    } else if (blockId.isRDD) {
      name = rddDir + "/" + blockId.name
    }
    return name
  }


  def blockExists(blockId: BlockId): Boolean = {
    val path = getPath(blockId)
    val fileInfo = fs.lookup(path).get()
    fileInfo != null

    /*
      val crailFile = getFile(blockId)
      crailFile.synchronized {
        var fileInfo = crailFile.getFile()
        if (fileInfo != null) {
          // Double-check the file in CrailFS
          val path = getPath(blockId)
          val remoteFileInfo = fs.lookup(path).get()
          if (remoteFileInfo == null) {
            crailFile.update(null)
            return false
          } else {
            return true
          }
        }
      }
    false
    */
  }
}

abstract class DisaggCachingPolicy(
           memoryStore: MemoryStore) {


  /**
   * Attempts to cache spilled values read from disagg into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  def maybeCacheDisaggValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      disaggIterator: Iterator[T]): Iterator[T]

  /**
   * Attempts to cache spilled bytes read from disagg into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  def maybeCacheDisaggBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      disaggData: BlockData): Option[ChunkedByteBuffer]


}

object DisaggCachingPolicy {

  def apply(conf: SparkConf,
            memoryStore: MemoryStore): DisaggCachingPolicy = {
     val cachingPolicyType = conf.get("spark.disagg.cachingpolicy", "None")
     cachingPolicyType match {
      case "None" => new NoCachingPolicy(memoryStore)
      case _ => throw new RuntimeException("Invalid caching policy " + cachingPolicyType)
    }
  }
}
