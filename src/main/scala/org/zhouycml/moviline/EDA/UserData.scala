package org.zhouycml.moviline.EDA

import org.zhouycml.moviline.utils.Utils

object UserData {
  def main(args: Array[String]): Unit = {
    val spark = Utils.spark
    val sc = spark.sparkContext

    val userFilePath = args(0)
    println(s"read file from: $userFilePath")
    val userDF = Utils.getUserDF(userFilePath)

    val first = userDF.first()
    println(s"First User Record:$first")

    val numGenders = userDF.groupBy("gender").count()
    val numOccupation = userDF.groupBy("occupation").count()
    val numZipcodes = userDF.groupBy("zipcode").count()

    println(s"num users: ${userDF.count()}")
    println("********* gender show*********")
    numGenders.show()
    println("************user show***********")
    numOccupation.show()

  }
}
