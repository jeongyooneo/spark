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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Dataset}
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating ALS.
 * Run with
 * {{{
 * bin/run-example ml.ALSExample
 * }}}
 */
object LastFmALSExample {

  // $example on$
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  // $example off$

  def buildCount(rawUserArtistData: Dataset[String],
                 bArtistAlias: Broadcast[Map[Int, Int]],
                 spark: SparkSession): DataFrame = {
    import spark.implicits._
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(" ").map(_.toInt)
      val finalArtistId = bArtistAlias.value.getOrElse(artistID, artistID)
      (userID, finalArtistId, count)
    }.toDF("user", "artist", "count")
  }

  def main(args: Array[String]) {
    @transient lazy val mylogger = org.apache.log4j.LogManager.getLogger("myLogger")
    val startTime = System.nanoTime

    val spark = SparkSession
      .builder
      .appName("LastFmALSExample")
      .getOrCreate()

    import spark.implicits._

    // $example on$
    val rawUserArtistData = spark.read.textFile("/dataset/lastfm/user_artist_data.txt")
      .persist()

    val userArtistDF = rawUserArtistData
      .map({ line =>
        val Array(user, artist, score) = line.split(' ')
        (user.toInt, artist.toInt)
      }).toDF("user", "artist")

    val artistById = spark.read.textFile("/dataset/lastfm/artist_data.txt")
      .flatMap({ line =>
        val (id, name) = line.span(_ != "\t")
        if (name.isEmpty) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case _: NumberFormatException => None
          }
        }
      }).toDF("id", "name")

    val artistAlias = spark.read.textFile("/dataset/lastfm/artist_alias.txt")
      .flatMap({ line =>
        val Array(artist, alias) = line.split("\t")
        if (artist.isEmpty) {
          None
        } else {
          Some((artist.toInt, alias.toInt))
        }
      }).collect().toMap

    val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
    val trainData = buildCount(rawUserArtistData, bArtistAlias, spark)

    rawUserArtistData.unpersist()

    val Array(training, test) = trainData.randomSplit(Array(0.8, 0.2))

    training.cache()

    for (i <- 0 to 3) {
      val model = new ALS()
        .setImplicitPrefs(true)
        .setRank(10)
        .setRegParam(0.01)
        .setAlpha(1.0)
        .setMaxIter(5)
        .setUserCol("user")
        .setItemCol("artist")
        .setRatingCol("count")
        .setPredictionCol("prediction")
        .fit(training)

      // Evaluate the model by computing the RMSE on the test data
      // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
      model.setColdStartStrategy("drop")

      // Generate top 10 movie recommendations for each user
      val userRecs = model.recommendForAllUsers(10)
      // Generate top 10 user recommendations for each movie
      val movieRecs = model.recommendForAllItems(10)
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

