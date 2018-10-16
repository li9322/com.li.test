package com.li.spark.scala.spqrksql

import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("").setMaster("")
    val sc=new SparkContext(conf)

  }
}
