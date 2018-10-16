package com.li.spark.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //非常重要，是通向spark集群的入口
    val conf=new SparkConf().setAppName("wordcont").setMaster("local[2]")
    val sc=new SparkContext(conf)
    //textFile会产生两个RDD：HadoopRDD -> MapPartitionsRDD
    sc.textFile(args(0))
      //产生一个RDD：MapPartitionsRDD
      .flatMap(_.split(" "))
      //产生一个RDD：MapPartitionsRDD
      .map((_,1))
      //产生一个RDD：ShuffledRDD
      .reduceByKey(_+_,numPartitions = 1)
      //产生一个RDD：ShuffledRDD
      .sortBy(_._2,false)
      //产生一个RDD：MapPartitionsRDD
      .saveAsTextFile(args(1))
    sc.stop()
  }

}
