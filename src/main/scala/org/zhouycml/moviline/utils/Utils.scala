package org.zhouycml.moviline.utils

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

  val customMovieSchema = StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("date", StringType, true),
    StructField("url", StringType, true)
  )
  )

  val customRatingScheme = StructType(Array(
    StructField("user_id", IntegerType, true),
    StructField("movie_id", IntegerType, true),
    StructField("rating", IntegerType, true),
    StructField("timestamp", IntegerType, true)
  ))

  val customUserSchema = StructType(Array(
    StructField("no", IntegerType, true),
    StructField("age", StringType, true),
    StructField("gender", StringType, true),
    StructField("occupation",StringType, true),
    StructField("zipcode", StringType, true)
  ))

  def getMovieDF(filePath: String):DataFrame={
    val df = spark.read.format("csv")
      .option("delimiter","|")
      .schema(customMovieSchema)
      .load(filePath)

    df
  }

  def getRatingDF(filePath:String):DataFrame={
    val df = spark.read.format("csv")
      .option("delimiter","\t")
      .schema(customRatingScheme)
      .load(filePath)
    df
  }

  def getUserDF(filePath:String):DataFrame={
    val df = spark.read.format("csv")
      .option("delimiter", "|")
      .schema(customUserSchema)
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