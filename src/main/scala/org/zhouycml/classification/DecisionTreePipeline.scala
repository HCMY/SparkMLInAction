package org.zhouycml.classification

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler,
                                    VectorIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger


import scala.collection.mutable

object DecisionTreePipeline {

  def decisionTreePipeline(vectorAssembler: VectorAssembler,
                           dataFrame: DataFrame): Unit ={

    val Array(train_set, val_set) = dataFrame.randomSplit(Array(0.8,0.2))

    val stage = new mutable.ArrayBuffer[PipelineStage]()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    stage += labelIndexer
    stage += vectorAssembler


    val dt = new DecisionTreeClassifier()
      .setFeaturesCol(vectorAssembler.getOutputCol)
      .setLabelCol("indexedLabel")
      .setMaxDepth(5)
      .setMaxBins(32)
      .setCheckpointInterval(10)
      .setMinInfoGain(0.0)
      .setCacheNodeIds(false)

    stage += dt

    val pipeline = new Pipeline().setStages(stage.toArray)

    val start_time = System.nanoTime()

    val model = pipeline.fit(train_set)

    val elapsed_time = (System.nanoTime() - start_time)/1e9
    println(s"Training time: $elapsed_time")

    val hold_out = model.transform(val_set)
    hold_out.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    println(s"Test set accuracy: ${evaluator.evaluate(hold_out)}")


  }
}
