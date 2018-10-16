package com.li.spark.scala.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.sql.{Dataset, SparkSession}

object SreamingWordCount {
  def main(args: Array[String]): Unit = {
//    val spark=SparkSession.builder().appName("SreamingWordCount").master("local").getOrCreate()
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(10))

    val lines: ReceiverInputDStream[String] =ssc.socketTextStream("192.168.70.11",1888)
    val words: DStream[String] =lines.flatMap(_.split(" "))
    val wordsAndOne: DStream[(String, Int)] =words.map((_,1))
    val reduced: DStream[(String, Int)] =wordsAndOne.reduceByKey(_+_)

    reduced.print()
    ssc.start()

    ssc.awaitTermination()
  }
}
