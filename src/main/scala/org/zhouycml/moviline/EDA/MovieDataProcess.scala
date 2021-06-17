package org.zhouycml.moviline.EDA

import org.zhouycml.moviline.utils.Utils

object MovieDataProcess {
  def main(args: Array[String]): Unit = {
    val spark = Utils.spark
    val sc=  spark.sparkContext

    val movieFilePath = args(0)


    val movieData = Utils.getMovieDF(filePath = movieFilePath)
    movieData.createOrReplaceTempView("m_data")
    movieData.printSchema()

    spark.udf.register("convertYear", Utils.convertYear _)
    movieData.show()

    val movieYears = spark.sql("select convertYear(date) as year from m_data")
    movieYears.show()
    movieYears.createOrReplaceTempView("m_years")
    spark.udf.register("replaceEmptyStr", Utils.replaceEmptyStr _)

    val yearReplaced = spark.sql("select replaceEmptyStr(year) as r_year from m_years")
    yearReplaced.show()

    val movieYearsFiltered = movieYears.filter(x => (x==1900))
    val movieYearsValid = yearReplaced.filter(x=>(x!=1900)).collect()
    val movieYearsValidInt = new Array[Int](movieYearsValid.length)
  }
}
