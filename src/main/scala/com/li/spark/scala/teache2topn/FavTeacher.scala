package com.li.spark.scala.teache2topn

import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc=new SparkContext(conf)
    //指定以后从哪里读取数据
    val lines=sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\" +
      "spark-03-TopN与WordCount执行过程详解\\课件与代码\\teacher.log")
    //整理数据
    val teacherAndOne=lines.map(line=>{
      val index=line.lastIndexOf("/")
      val teacher=line.substring(index+1)
      (teacher,1)
    })
    //聚合
    val reduced=teacherAndOne.reduceByKey(_+_,numPartitions = 1)
    //排序
    val sorted=reduced.sortBy(_._2,false)
    //触发Action执行计算
    val result=sorted.collect().take(3)
    //打印
    println(result.toBuffer)
    sc.stop()
  }
}
