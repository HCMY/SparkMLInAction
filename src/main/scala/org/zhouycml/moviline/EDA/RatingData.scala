package org.zhouycml.moviline.EDA

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.zhouycml.moviline.utils.Utils

object RatingData {
  def main(args: Array[String]): Unit = {
    val ratingPath = args(0)
    val spark = Utils.spark

    val ratingDF = Utils.getRatingDF(ratingPath)
    println("***** rating data show*****")
    ratingDF.show()

    val num_ratings = ratingDF.count()
    println(s"num ratings: $num_ratings")

    ratingDF.createOrReplaceTempView("rating_df")
    val maxRate = spark.sql("select max(rating) from rating_df")
    val minRate = spark.sql("select min(rating) from rating_df")
    val avgRate = spark.sql("select avg(rating) from rating_df")
    println("******** statistics show***************")
    maxRate.show()
    minRate.show()
    avgRate.show()

    val ratingGrouped = ratingDF.groupBy("rating")
    println("***************rating group show*******")
    ratingGrouped.count().show()

  }
}
