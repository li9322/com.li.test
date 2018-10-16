package com.li.spark.scala.teache2topn

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher2Itcast {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc=new SparkContext(conf)
    //指定以后从哪里读取数据
    val lines=sc.textFile("H:\\大数据第三期\\day29\\itcast.log")
    val urlAndOne=lines.map(line => {
      val file = line.split("\t")
      (file(1),1)
    })

    val urlreduceed=urlAndOne.reduceByKey(_+_)
    val teacherAndCount=urlreduceed.map(courseAndCount=>{
      val url=courseAndCount._1
      val host=new URL(url).getHost
      (host,url,courseAndCount._2)
    })
    val result=teacherAndCount.groupBy(_._1).mapValues(it=>{
      it.toList.sortBy(_._3).reverse.take(3)
    })
    println(result.collect().toBuffer)
    sc.stop()
  }
}
