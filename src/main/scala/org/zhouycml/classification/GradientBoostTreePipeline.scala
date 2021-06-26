package org.zhouycml.classification

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


object GradientBoostTreePipeline {
  def gradientBoostTreePipeline(dataFrame: DataFrame): Unit ={
    val Array(train_set, test_set) = dataFrame.randomSplit(Array(0.8, 0.2))

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dataFrame)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(dataFrame)

    val gbdt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxDepth(5)
      .setMaxIter(30)

    val pipeline = new Pipeline().setStages(Array(labelIndexer,
                                                  featureIndexer,
                                                   gbdt))
    val start_time = System.nanoTime()

    val model = pipeline.fit(train_set)

    val elapsed_time = (System.nanoTime() - start_time)/1e9
    println(s"Training time: $elapsed_time")

    val predictions = model.transform(test_set)

    test_set.show(50)
    predictions.select("prediction", "label", "indexedLabel","features").show(50)
    predictions.select("label").groupBy("label").count().show()
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test accuracy = $accuracy")
  }
}
