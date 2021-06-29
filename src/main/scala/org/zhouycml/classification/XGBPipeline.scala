package org.zhouycml.classification

import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier

object XGBPipeline {
  def xgbPipeline(dataFrame: DataFrame): Unit ={
    val Array(trainSet, valSet) = dataFrame.randomSplit(Array(0.8, 0.2))

    val xgbParam = Map("eta" -> 0.1f,
                        "missing"->0.0,
                        "objective"->"binary:logistic",
                        "num_round"->100,
                        "num_workers"->2)

    val xgb = new XGBoostClassifier(xgbParam)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setEvalMetric("auc")
      .setMaxDepth(5)

    val model = xgb.fit(trainSet)

    val predictions = model.transform(valSet).select("prediction","label")
    predictions.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("label")
      .setMetricName("accuracy")

    val acc = evaluator.evaluate(predictions)
    println(s"Test accuracy: $acc")


  }

}
