package com.li.spark.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object Testclass {
  def main(args: Array[String]): Unit = {
    val config=new SparkConf()
    val sc=new SparkContext(config)
    sc.textFile(args(0)).map(x=>{(x.split(" ")(0),1)}).reduceByKey(_+_).collect()
    sc.stop()
  }
}
