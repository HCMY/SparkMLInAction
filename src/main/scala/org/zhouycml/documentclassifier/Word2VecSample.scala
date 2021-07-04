package org.zhouycml.documentclassifier

import org.apache.spark.ml.feature.Word2Vec

import org.zhouycml.moviline.utils.Utils

object Word2VecSample {
  def main(args: Array[String]): Unit = {
    val spark = Utils.spark
    val sc = spark.sparkContext

    val docDF1 = spark.createDataFrame(Seq(
      "Hi I heard about spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply))
    docDF1.show()

    val docDF2 = spark.createDataFrame(Seq(
      "Hi I heard about spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    docDF2.show()

    val w2v = new Word2Vec()
        .setInputCol("text")
        .setOutputCol("result")
        .setVectorSize(16)
        .setMinCount(0)

    val model = w2v.fit(docDF2)
    val result = model.transform(docDF2)
    result.show()
    result.select("result").take(3).foreach(x=>println(x.getClass))
    result.select("result").take(3).foreach(x=>println(x))

    sc.stop()
  }

}
