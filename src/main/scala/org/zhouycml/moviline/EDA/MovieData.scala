package org.zhouycml.moviline.EDA

import org.apache.spark.sql.{DataFrame}

import org.zhouycml.moviline.utils.Utils

object MovieData {

  def movieYearsCnt(df:DataFrame): Unit ={
    val spark = Utils.spark

    println("*********movie data cols*************")
    println(Utils.getFields(df))
    println("********movie df show*********")
    df.show()
    df.createOrReplaceTempView("movie_data")
    spark.udf.register("convertYear", Utils.convertYear _)
    val movieYears = spark.sql("select convertYear(date) as year from movie_data")
    println("************move years show*************")
    movieYears.show()

    println("************ move cnt show******************")
    val movieYearsCount = movieYears.groupBy("year").count()
    movieYearsCount.show()
  }

  def main(args: Array[String]): Unit = {
    val movieDataPath = args(0)
    println(s"read movie data from$movieDataPath")
    val movieDataDF = Utils.getMovieDF(movieDataPath)
    movieYearsCnt(movieDataDF)
  }
}
