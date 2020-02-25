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
import org.apache.spark.storage.disagg.DisaggBlockManagerEndpoint

import scala.collection.mutable.ListBuffer

class BenefitAnalyzer()
  extends Logging {

  var  disaggBlockManagerEndpoint: Option[DisaggBlockManagerEndpoint] = None

  def setDisaggBlockManagerEndpoint(disaggM: DisaggBlockManagerEndpoint): Unit = {
    disaggBlockManagerEndpoint = Some(disaggM)
  }

  val window: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]
  var prevBenefitVal: (Long, Long) = (0L, 1L)

  var consecutive = 0
  var prevEvictTime = System.currentTimeMillis()

  val windowSize: Int = 10


  private def findMax: (Int, (Long, Long)) = {
    var maxIndex = 0
    var maxVal: (Long, Long) = (0L, 1L)
    var cnt = 0
    window.foreach(v => {
      if (maxVal._1.toDouble / maxVal._2 < v._1.toDouble / v._2) {
        maxVal = v
        maxIndex = cnt
      }
      cnt += 1
    })

    (maxIndex, maxVal)
  }

  def analyze(compReduction: Long,
              totalSize: Long): Unit = {


    /*
    logInfo(s"$window")

    // delta
    if (window.size < windowSize) {
      window.append((compReduction, totalSize))
    } else {
      // find max
      val (index, maxVal) = findMax

      // find delta
      val timeElapsed = (windowSize - index) * 2

      val delta = ((maxVal._1.toDouble / maxVal._2)
        - (compReduction.toDouble / totalSize))

      if (delta > 0.35) {
        // reduce size !!
        logInfo(s"Delta for benefit $delta, max: ${maxVal._1.toDouble / maxVal._2}" +
          s", curr: ${compReduction.toDouble / totalSize}")
        disaggBlockManagerEndpoint match {
          case None =>
          case Some(manager) => manager.evictBlocksToIncreaseBenefit(compReduction, totalSize)
        }
      }

      window.remove(0)
      window.append((compReduction, totalSize))
    }
    */


    val currBenefit = compReduction.toDouble / totalSize
    val prevBenefit = prevBenefitVal._1.toDouble / prevBenefitVal._2

    window.append((compReduction, totalSize))

    if (prevBenefit > currBenefit) {
      consecutive += 1
    } else {
      consecutive = 0
      window.clear()
    }

    if (consecutive >= 3) {
      val (index, maxVal) = findMax

      val delta = ((maxVal._1.toDouble / maxVal._2)
        - (compReduction.toDouble / totalSize))

      if (delta > 0.5) {
        // evict !!
        prevEvictTime = System.currentTimeMillis()
        logInfo(s"Start evicting to increase benefit!! ${compReduction}, $totalSize")

        disaggBlockManagerEndpoint match {
          case None =>
          case Some(manager) => manager.evictBlocksToIncreaseBenefit(compReduction, totalSize)
        }
      }

      consecutive = 0
      window.clear()
    }

    prevBenefitVal = (compReduction, totalSize)
  }
}

