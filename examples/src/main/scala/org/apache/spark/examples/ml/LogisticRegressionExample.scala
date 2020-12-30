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

import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

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

  case class Params(
      input: String = null,
      testInput: String = "",
      dataFormat: String = "libsvm",
      regParam: Double = 0.1,
      elasticNetParam: Double = 0.3,
      maxIter: Int = 10,
      fitIntercept: Boolean = true,
      tol: Double = 0.1,
      fracTest: Double = 0.2) extends AbstractParams[Params]

  def main(args: Array[String]) {
    run(args)
  }

  def run(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"LogisticRegressionExample")
      .getOrCreate()

    // Load training and test data and cache it.
    // val (training: DataFrame, test: DataFrame) = DecisionTreeExample.loadDatasets(params.input,
    //  params.dataFormat, params.testInput, "classification", params.fracTest)

    val triazines = spark.read.format("libsvm").load(args(0))

    // Split the data into training and test sets (30% held out for testing).
    val Array(training, test) = triazines.randomSplit(Array(0.85, 0.15), seed = 1)

    // Set up Pipeline.
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
    stages += labelIndexer

    val lor = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setRegParam(0.1)
      .setElasticNetParam(0.3)
      .setMaxIter(10)
      .setTol(0.1)
      .setFitIntercept(true)

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
    // DecisionTreeExample.evaluateClassificationModel(pipelineModel, training, "indexedLabel")
    println("Test data results:")
    // DecisionTreeExample.evaluateClassificationModel(pipelineModel, test, "indexedLabel")

    spark.stop()
  }
}
// scalastyle:on println
