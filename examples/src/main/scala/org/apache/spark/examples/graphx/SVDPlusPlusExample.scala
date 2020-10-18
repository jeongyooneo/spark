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

package org.apache.spark.examples.graphx

import org.apache.spark.graphx.{Edge, GraphLoader}
import org.apache.spark.graphx.lib.SVDPlusPlus
import org.apache.spark.sql.SparkSession

object SVDPlusPlusExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the graph as in the PageRank example
    val svdppErr = 8.0
    val inputPath = args(0)

    val edges = sc.textFile(inputPath).map { line =>
      val fields = line.split("::")
      Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
    }
    val conf = new SVDPlusPlus.Conf(10, 5, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015) // 5 iterations
    val (graph, _) = SVDPlusPlus.run(edges, conf)
    graph.cache()

    val err = graph.vertices.map { case (vid, vd) =>
      if (vid % 2 == 1) vd._4 else 0.0
    }.reduce(_ + _) / graph.numEdges
    // Print the result
    println(s"error $err, goal was $svdppErr")

    spark.stop()
  }
}


