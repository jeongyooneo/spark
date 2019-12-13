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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.recommendation.ALS
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating ALS.
 * Run with
 * {{{
 * bin/run-example ml.ALSExample
 * }}}
 */
object ALSExample {

  // $example on$
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  // $example off$

  def main(args: Array[String]) {
    @transient lazy val mylogger = org.apache.log4j.LogManager.getLogger("myLogger")
    val startTime = System.nanoTime

    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val ratings = spark.read.textFile("data/mllib/als/output")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    training.cache()

    // Build the recommendation model using ALS on the training data
    for (i <- 0 to 2) {
      val als = new ALS()
        .setMaxIter(3)
        .setRegParam(0.01)
        .setRank(10)
        .setUserCol("userId")
        .setItemCol("movieId")
        .setRatingCol("rating")
        .setImplicitPrefs(true)
      val model = als.fit(training)

      // Evaluate the model by computing the RMSE on the test data
      // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
      model.setColdStartStrategy("drop")
      val predictions = model.transform(test)

      /*
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    */

      // Generate top 10 movie recommendations for each user
      val userRecs = model.recommendForAllUsers(5)
      // Generate top 10 user recommendations for each movie
      val movieRecs = model.recommendForAllItems(5)
      // $example off$
      // userRecs.show()
      // movieRecs.show()

      // Thread.sleep(1000000)
      val jct = (System.nanoTime() - startTime) / 1000000
      mylogger.info(s"Iteration ${i + 1} finished  $jct ms")
    }

    spark.stop()
    val jct = (System.nanoTime() - startTime) / 1000000
    mylogger.info("ALS JCT: " + jct + " ms")
  }
}
// scalastyle:on println

