package org.zhouycml.documentclassifier

import org.apache.spark.rdd.RDD
import org.zhouycml.moviline.utils.Utils

object TFIDFExtraction {
  val rareTokens = Set("")
  val stopWords = Set(
    "the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if",
    "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
  )
  val regex = """[^0-9]*""".r

  def tokenizer(line:String):Seq[String]={
    line.split("""\W+""")
      .map(_.toLowerCase)
      .filter(token => regex.pattern.matcher(token).matches())
      .filterNot(token => stopWords.contains(token))
      .filterNot(token=>rareTokens.contains(token))
      .filter(token=>token.size >= 2).toSeq
  }


}
