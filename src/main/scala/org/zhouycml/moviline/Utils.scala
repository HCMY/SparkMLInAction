package org.zhouycml.moviline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame}

object Utils {

  val spark = SparkSession
    .builder()
    .appName("Utils")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  val customSchema = StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("date", StringType, true),
    StructField("url", StringType, true)
    )
  )

  def getMovieDF(filePath: String):DataFrame={
    val df = spark.read.format("csv")
      .option("delimiter","|")
      .schema(customSchema)
      .load(filePath)

    df
  }

  def numSamples(df:DataFrame):Long={
    df.count()
  }

  def getFields(df:DataFrame):String={
    df.columns.toArray.mkString("\t")
  }

  def convertYear(x:String):Int={
    try
      return x.takeRight(4).toInt
    catch {
      case e:Exception => println("return 1900")
        1900
    }
  }









}
