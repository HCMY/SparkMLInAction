package org.zhouycml.documentclassifier

import org.apache.spark.mllib.feature.Word2Vec

import org.zhouycml.moviline.utils.Utils

/*
mlib for rdd
 */

object Word2VecMlib {
  def main(args: Array[String]): Unit = {
    val spark = Utils.spark
    val sc = spark.sparkContext

    val inputFilePath = args(0)

    val rdd = sc.wholeTextFiles(inputFilePath)
    val text = rdd.map {case(file, text) => text}
    val newsGroups = rdd.map {case (file, text) => file.split("/").takeRight(2).head}
    val newsGroupsMap = newsGroups.distinct().collect().zipWithIndex.toMap
    val dim = math.pow(2, 18)
    val tokens = text.map(doc => TFIDFExtraction.tokenizer(doc))
    val w2v = new Word2Vec()
    val w2vModel = w2v.fit(tokens)
    w2vModel.findSynonyms("philosophers", 5).foreach(println)

    sc.stop()

  }
}
