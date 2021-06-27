package org.zhouycml.classification

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{VectorIndexer, VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline


object LogisticRegressionPipeline {
  def logisticRegressionPipeline(vectorAssembler: VectorAssembler,
                                 dataFrame: DataFrame): Unit ={
    val lr = new LogisticRegression()
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75))
      .build()

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, lr))

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val Array(train_set, test_set) = dataFrame.randomSplit(Array(0.8, 0.2))

    val model = trainValidationSplit.fit(train_set)

    val prediction = model.transform(test_set).select("prediction","label")

    val metrics = new RegressionMetrics(prediction.rdd.map(x=>(x(0).asInstanceOf[Double],
    x(1).asInstanceOf[Double])))

    println(s"Test r2: ${metrics.r2}, mse: ${metrics.meanSquaredError}")

    val samples = test_set.count()
    val correct = prediction.rdd.map(x=>
      if (x(0).asInstanceOf[Double]==x(1).asInstanceOf[Double]) 1 else 0).sum()
    val accuracy = correct/samples
    println(s"Test accuracy: $accuracy")


  }

}
