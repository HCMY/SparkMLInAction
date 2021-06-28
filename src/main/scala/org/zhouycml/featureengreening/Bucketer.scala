package org.zhouycml.featureengreening

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.feature.Bucketizer
import scala.collection.mutable.ArrayBuffer


class Bucketer() {
  private var numBuckets = 255
  private var savePath = ""
  private var labelColName = ""
  private var  model = ArrayBuffer[Bucketizer]()


  def setNumBuckets(value:Int): this.type ={
    numBuckets = value
    this
  }

  def setLabelCol(value:String):this.type={
    labelColName = value
    this
  }

  def setSavePath(value:String):this.type={
    this
  }

  def loadModel(modelPath:String): this.type ={
    model.append(Bucketizer.load(modelPath))
    this
  }


  def transform(dataFrame: DataFrame):DataFrame={
    val trainedModel:Bucketizer = model(0)
    val inputCols = trainedModel.getInputCols

    val bucketedDF = trainedModel.transform(dataFrame).drop(inputCols:_*)

    bucketedDF
  }




  def autoDetectColType(dataFrame: DataFrame): Array[String] ={
    val collectedCols = new ArrayBuffer[String]()
    val colTypeMap = dataFrame.dtypes.toMap

    for((colName, colType) <- colTypeMap){
      if(colType != "String" && colName!=labelColName){
        collectedCols.append(colName)
      }
    }
    collectedCols.toArray
  }

  def fit(dataFrame: DataFrame):Bucketizer ={
    val bucketCols = autoDetectColType(dataFrame)
    val bucketedCols = bucketCols.map(x=> s"${x}_bucket")
    println(bucketCols)
    println(bucketedCols)
    println(s"numbuckers: $numBuckets")

    val quantileDiscretizer = new QuantileDiscretizer()
      .setInputCols(bucketCols)
      .setOutputCols(bucketedCols)
      .setNumBuckets(numBuckets)

    val model = quantileDiscretizer.fit(dataFrame)
    model
  }



}
