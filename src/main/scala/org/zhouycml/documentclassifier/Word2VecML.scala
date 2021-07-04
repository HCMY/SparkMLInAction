package org.zhouycml.documentclassifier

import org.apache.spark.ml.feature.Word2Vec

import org.zhouycml.moviline.utils.Utils

object Word2VecML {
  case class Record(name: String)

  def main(args: Array[String]): Unit = {
    val spark = Utils.spark
    val sc = spark.sparkContext

    val inputFileFolder = args(0)
    val rawDF = sc.wholeTextFiles(inputFileFolder)

    val temp = rawDF.map(x=> {x._2.filter(_ >= ' ').filter(! _.toString.startsWith("("))})
    val textDF = spark.createDataFrame(temp.map(x=>x.split(" ")).map(Tuple1.apply)).toDF("text")
    textDF.show()

    val w2v = new Word2Vec()
        .setInputCol("text")
        .setOutputCol("result")
        .setMinCount(0)
        .setVectorSize(16)

    val w2vModel = w2v.fit(textDF)

    val result = w2vModel.transform(textDF)
    result.select("result").take(5).foreach(x=>println(x))
    val ds = w2vModel.findSynonyms("philosophers", 5).select("word")
    ds.show()

    sc.stop()


  }

}
