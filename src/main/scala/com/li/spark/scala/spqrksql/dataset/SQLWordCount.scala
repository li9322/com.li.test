package com.li.spark.scala.spqrksql.dataset

import org.apache.spark.sql.{Dataset, SparkSession}

object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SQLWordCount").master("local").getOrCreate()
//    配置属性：
//    spark.conf.set("spark.sql.shuffle.partitions",6)
//    spark.conf.set("spark.executor.memory","2g")
    val lines: Dataset[String] =spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-06-spark SQL简介与案例详解1\\课件与代码\\wordcount.txt")
    import spark.implicits._
    val words: Dataset[String] =lines.flatMap(_.split(" "))
    words.createTempView("v_wc")
    val result= spark.sql("select value word,count(*) counts from v_wc group by word order by counts desc ")

    result.show()
    spark.stop()
  }
}
