package org.zhouycml

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object HelloSpark{
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    println(s"reading file from $inputPath")
    println(s"output file to $outputPath")

    val spark = SparkSession.builder
       .master("local[2]")
       .appName("Hello Spark App")
      .getOrCreate()
    val sc = spark.sparkContext

    val data = sc.textFile(inputPath)
      .map(line => line.split(','))
      .map(record => (record(0), record(1), record(2)))

    val dataDF = spark.createDataFrame(data)
    dataDF.show()

    val numPurchase = data.count()
    val uniqueUsers = data.map {case (user, product, price) => user}.distinct().count()
    val totalRevenue = data.map{case (user, product, price) => price.toDouble}.sum()

    val productsByPopulary = data.map {
      case (user, product, price) =>
        (product, 1)
    }.reduceByKey(_+_).collect().sortBy(-_._2)

    val mostPopular = productsByPopulary(0)

    println(s"Total purchases: $numPurchase")
    println(s"Unique Users: $uniqueUsers")
    println(s"Total Revenue: $totalRevenue")
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))
  }
}

