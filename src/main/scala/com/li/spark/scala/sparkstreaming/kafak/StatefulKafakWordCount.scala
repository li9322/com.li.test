package com.li.spark.scala.sparkstreaming.kafak

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulKafakWordCount {

  /**
    * 第一个参数：聚合的key，就是单词
    * 第二个参数：当前批次产生批次该单词在每一个分区出现的次数
    * 第三个参数：初始值或累加的中间结果
    */
  val updateFunc=(iter:Iterator[(String,Seq[Int],Option[Int])])=>{
//    iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
    iter.map{case (x,y,z) =>(x,y.sum+z.getOrElse(0))}
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafakWordCount").setMaster("local[2]")
    val sc=new SparkContext(conf)
//    sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("D:\\test")

    val zkQuorum = "hadoop02:2181,hadoop03:2181,hadoop04:2181"
    val groupId = "h1"
    val topic = Map[String, Int]("second" -> 1)
    //创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    //对数据进行处理
    //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)
    //对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //聚合
//    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //打印结果(Action)
    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
