package org.zhouycml.classification

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.ml.feature.VectorAssembler
import org.spark_project.jetty.io.ByteBufferPool.Bucket

import scopt.OptionParser

import org.zhouycml.classification.{DecisionTreePipeline, GradientBoostTreePipeline, LogisticRegressionPipeline, RandomForestPipeline, XGBPipeline}
import org.zhouycml.featureengreening.Bucketer



case class Params(
                inputFilePath:String="",
                outputFilePath:String="",
                modelSavePath:String="",
                algoName:String="")

object Executor {
  val parser = new OptionParser[Params]("argparser") {
    opt[String]('i', "input").required().action {
      (x, c) => c.copy(inputFilePath = x)
    }.text("file path for training")

    opt[String]('o',"output").optional().action{
      (x,c)=>c.copy(outputFilePath = x)
    }.text("output file path")

    opt[String]('m',"model").optional().action{
      (x,c)=>c.copy(modelSavePath = x)
    }.text("model save path")

    opt[String]('a',"alg").required().action{
      (x,c)=>c.copy(algoName = x)
    }.text("algorithms name, [gbdt,rf,bucket,lr]")
  }

  def main(args: Array[String]): Unit = {

    val params = parser.parse(args, Params()).get

    val spark = new SparkSession
    .Builder()
      .appName("classification")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    val filePath = params.inputFilePath
    println(s"read file from :$filePath")

    val df = spark.read.format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

    df.createOrReplaceTempView("data")
    df.printSchema()
    spark.sql("select * from data where alchemy_category = '?'").show()


    val df1 = df.withColumn("avglinksize", df("avglinksize").cast("double"))
      .withColumn("commonlinkratio_1", df("commonlinkratio_1").cast("double"))
      .withColumn("commonlinkratio_2", df("commonlinkratio_2").cast("double"))
      .withColumn("commonlinkratio_3", df("commonlinkratio_3").cast("double"))
      .withColumn("commonlinkratio_4", df("commonlinkratio_4").cast("double"))
      .withColumn("compression_ratio", df("compression_ratio").cast("double"))
      .withColumn("embed_ratio", df("embed_ratio").cast("double"))
      .withColumn("framebased", df("framebased").cast("double"))
      .withColumn("frameTagRatio", df("frameTagRatio").cast("double"))
      .withColumn("hasDomainLink", df("hasDomainLink").cast("double"))
      .withColumn("html_ratio", df("html_ratio").cast("double"))
      .withColumn("image_ratio", df("image_ratio").cast("double"))
      .withColumn("is_news", df("is_news").cast("double"))
      .withColumn("lengthyLinkDomain", df("lengthyLinkDomain").cast("double"))
      .withColumn("linkwordscore", df("linkwordscore").cast("double"))
      .withColumn("news_front_page", df("news_front_page").cast("double"))
      .withColumn("non_markup_alphanum_characters", df("non_markup_alphanum_characters").cast("double"))
      .withColumn("numberOfLinks", df("numberOfLinks").cast("double"))
      .withColumn("numwords_in_url", df("numwords_in_url").cast("double"))
      .withColumn("parametrizedLinkRatio", df("parametrizedLinkRatio").cast("double"))
      .withColumn("spelling_errors_ratio", df("spelling_errors_ratio").cast("double"))
      .withColumn("label", df("label").cast("double"))
    df1.printSchema()

    val replaceFunc = udf {(x:Double)=> if(x =="?") 0.0 else x}

    val df2 = df1.withColumn("avglinksize", replaceFunc(df1("avglinksize")))
      .withColumn("commonlinkratio_1", replaceFunc(df1("commonlinkratio_1")))
      .withColumn("commonlinkratio_2", replaceFunc(df1("commonlinkratio_2")))
      .withColumn("commonlinkratio_3", replaceFunc(df1("commonlinkratio_3")))
      .withColumn("commonlinkratio_4", replaceFunc(df1("commonlinkratio_4")))
      .withColumn("compression_ratio", replaceFunc(df1("compression_ratio")))
      .withColumn("embed_ratio", replaceFunc(df1("embed_ratio")))
      .withColumn("framebased", replaceFunc(df1("framebased")))
      .withColumn("frameTagRatio", replaceFunc(df1("frameTagRatio")))
      .withColumn("hasDomainLink", replaceFunc(df1("hasDomainLink")))
      .withColumn("html_ratio", replaceFunc(df1("html_ratio")))
      .withColumn("image_ratio", replaceFunc(df1("image_ratio")))
      .withColumn("is_news", replaceFunc(df1("is_news")))
      .withColumn("lengthyLinkDomain", replaceFunc(df1("lengthyLinkDomain")))
      .withColumn("linkwordscore", replaceFunc(df1("linkwordscore")))
      .withColumn("news_front_page", replaceFunc(df1("news_front_page")))
      .withColumn("non_markup_alphanum_characters", replaceFunc(df1("non_markup_alphanum_characters")))
      .withColumn("numberOfLinks", replaceFunc(df1("numberOfLinks")))
      .withColumn("numwords_in_url", replaceFunc(df1("numwords_in_url")))
      .withColumn("parametrizedLinkRatio", replaceFunc(df1("parametrizedLinkRatio")))
      .withColumn("spelling_errors_ratio", replaceFunc(df1("spelling_errors_ratio")))
      .withColumn("label", replaceFunc(df1("label")))

    df2.printSchema()

    val df3 = df2.drop("url")
      .drop("urlid")
      .drop("boilerplate")
      .drop("alchemy_category")
      .drop("alchemy_category_score")

    val df4 = df3.na.fill(0.0)

    df4.createOrReplaceTempView("StumbleUpon_PreProc")
    spark.sql("select * from StumbleUpon_PreProc limit 10").show()
    df4.printSchema()

    val assembler = new VectorAssembler()
      .setInputCols(Array("avglinksize", "commonlinkratio_1", "commonlinkratio_2",
                          "commonlinkratio_3", "commonlinkratio_4", "compression_ratio",
                          "embed_ratio", "framebased", "frameTagRatio", "hasDomainLink",
                          "html_ratio", "image_ratio","is_news", "lengthyLinkDomain",
                          "linkwordscore", "news_front_page", "non_markup_alphanum_characters",
                          "numberOfLinks",
                          "numwords_in_url", "parametrizedLinkRatio", "spelling_errors_ratio"))
      .setOutputCol("features")

    val df_last = assembler.transform(df4).select("features","label")
    df_last.show()

    val model_name = params.algoName

    if(model_name == "gbdt") {
      /*
      var tmp_df = df4.withColumn("index", monotonically_increasing_id())
        .select("label", "index")
      var tmp_feature = assembler.transform(df4)
        .withColumn("index", monotonically_increasing_id())
        .select("features", "index")

      val df_last = tmp_df.as("df1").join(tmp_feature.as("df2"),
        tmp_df("index") === tmp_feature("index"), "inner")
        .select("df1.label", "df2.features")
       */
      GradientBoostTreePipeline.gradientBoostTreePipeline(dataFrame = df_last)
    }
    else{
        model_name match {
          case "dt" =>
            DecisionTreePipeline.decisionTreePipeline(vectorAssembler = assembler ,dataFrame = df4)

          case "lr" =>
            LogisticRegressionPipeline.logisticRegressionPipeline(assembler, df4)

          case "rf"=>
            val prediction_save_path = params.outputFilePath
            val model_save_path = params.modelSavePath
            println(s"model save path: $model_save_path")
            println(s"predcition save path: $prediction_save_path")
            val rf = new RandomForestPipeline(prediction_save_path, model_save_path)
            rf.randomForestPipeline(dataFrame = df_last)

          case "xgb" => XGBPipeline.xgbPipeline(dataFrame = df_last)
          case "xgb-buk" =>
            val bucketer = new Bucketer()
              .setLabelCol("label")
              .setNumBuckets(200)
            val dfBucketed = bucketer.fit(df4).transform(df4)
            val bucketedCols = bucketer.getBucketedCols()
            val assember = new VectorAssembler()
              .setInputCols(bucketedCols)
              .setOutputCol("features")
            val fitDF = assember.transform(dfBucketed).select("features","label")
            XGBPipeline.xgbPipeline(dataFrame = fitDF)

          case "bucket" =>
            val bucketer = new Bucketer()
              //.setNumBuckets(100)
              //.setLabelCol("label")
            //val model = bucketer.fit(df4)
            //model.save(args(2))
            val bucketedDF = bucketer.loadModel(params.modelSavePath).transform(df4)

            bucketedDF.show()
          case _ =>
            println("invalid algorithm name")
        }
      }

    sc.stop()
  }
}
