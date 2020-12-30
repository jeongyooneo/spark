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

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * An example runner for logistic regression with elastic-net (mixing L1/L2) regularization.
 * Run with
 * {{{
 * bin/run-example ml.LogisticRegressionExample [options]
 * }}}
 * A synthetic dataset can be found at `data/mllib/sample_libsvm_data.txt` which can be
 * trained by
 * {{{
 * bin/run-example ml.LogisticRegressionExample --regParam 0.3 --elasticNetParam 0.8 \
 *   data/mllib/sample_libsvm_data.txt
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LogisticRegressionExample {
  /** Load a dataset from the given path, using the given format */
  def loadData(spark: SparkSession,
               path: String,
               format: String,
               expectedNumFeatures: Option[Int] = None): DataFrame = {
    import spark.implicits._

    format match {
      case "dense" => MLUtils.loadLabeledPoints(spark.sparkContext, path).toDF()
      case "libsvm" => expectedNumFeatures match {
        case Some(numFeatures) => spark.read.option("numFeatures", numFeatures.toString)
          .format("libsvm").load(path)
        case None => spark.read.format("libsvm").load(path)
      }
      case _ => throw new IllegalArgumentException(s"Bad data format: $format")
    }
  }

  def loadDatasets(input: String,
                   dataFormat: String,
                   testInput: String,
                   fracTest: Double): (DataFrame, DataFrame) = {

    val spark = SparkSession
      .builder
      .getOrCreate()

    // Load training data
    val origExamples: DataFrame = loadData(spark, input, dataFormat)

    // Load or create test set
    val dataframes: Array[DataFrame] = if (testInput != "") {
      // Load testInput.
      val numFeatures = origExamples.first().getAs[Vector](1).size
      val origTestExamples: DataFrame =
        loadData(spark, testInput, dataFormat, Some(numFeatures))
      Array(origExamples, origTestExamples)
    } else {
      // Split input into training, test.
      origExamples.randomSplit(Array(1.0 - fracTest, fracTest), seed = 12345)
    }

    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")

    (training, test)
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"LogisticRegressionExample")
      .getOrCreate()

    var input = "/HiBench/LR/Input"
    if (args.length > 0) {
      input = args(0)
    }

    // Load training and test data and cache it.
    val dataFormat = "dense"
    val testInput = ""
    val fracTest = 0.2
    val regParam = 0.0
    val elasticNetParam = 0.0
    val maxIter = 100
    val fitIntercept = true
    val tol = 1E-6

    val (training: DataFrame, test: DataFrame) =
      loadDatasets(input, dataFormat, testInput, fracTest)

    // Set up Pipeline.
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
    stages += labelIndexer

    val lor = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFitIntercept(fitIntercept)

    stages += lor
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipelineModel = pipeline.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

    println("Training data results:")
    DecisionTreeExample.evaluateClassificationModel(pipelineModel, training, "indexedLabel")
    println("Test data results:")
    DecisionTreeExample.evaluateClassificationModel(pipelineModel, test, "indexedLabel")

    spark.stop()
  }
}
// scalastyle:on println

