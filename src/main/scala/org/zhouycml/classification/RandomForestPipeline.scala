package org.zhouycml.classification

import org.apache.spark.ml.classification.{RandomForestClassifier,RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame


class RandomForestPipeline(prediction_save_path:String,
                           model_save_path:String) {

  val pre_save_path = prediction_save_path
  val mod_save_path = model_save_path


  def randomForestPipeline(dataFrame: DataFrame): Unit ={
    val Array(train_set, val_set) = dataFrame.randomSplit(Array(0.8, 0.2))

    val rf = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxDepth(5)
      .setNumTrees(40)
      .setMinInfoGain(0.0)

    val model = rf.fit(train_set)
    val predictions = model.transform(val_set).select("prediction","label")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val acc = evaluator.evaluate(predictions)
    println(s"Test accuracy: $acc")

    savePredictions(predictions)
    saveModel(model)
  }

  def savePredictions(dataFrame: DataFrame): Unit ={
    dataFrame.coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(pre_save_path)
  }

  def saveModel(model: RandomForestClassificationModel): Unit ={
    model.save(mod_save_path)
    println("model save successful...")
  }
}
