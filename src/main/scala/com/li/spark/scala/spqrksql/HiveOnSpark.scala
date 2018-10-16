package com.li.spark.scala.spqrksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local")
      .enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法)
      .getOrCreate()
    val result=spark.sql("select * from t_access order by fv desc")
    result.show()
    spark.stop()
  }
}