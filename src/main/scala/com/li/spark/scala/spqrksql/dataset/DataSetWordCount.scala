package com.li.spark.scala.spqrksql.dataset

import org.apache.spark.sql.{Dataset, SparkSession}

object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SQLWordCount").master("local").getOrCreate()
    val lines: Dataset[String] =spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-06-spark SQL简介与案例详解1\\课件与代码\\wordcount.txt")
    import spark.implicits._
    val words: Dataset[String] =lines.flatMap(_.split(" "))

//    val result: Dataset[Row] =words.groupBy($"value" as "word").count().sort($"count" desc)
    import org.apache.spark.sql.functions._
    val result=words.groupBy($"value" as "word").agg(count("*") as "counts").sort($"counts" desc)

    result.show()
    spark.stop()
  }
}
