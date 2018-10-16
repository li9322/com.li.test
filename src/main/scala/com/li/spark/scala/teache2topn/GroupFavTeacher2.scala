package com.li.spark.scala.teache2topn

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc=new SparkContext(conf)
    val subjects = Array("bigdata", "javaee", "php")

    sc.setCheckpointDir("hdfs://hadoop01:9000/spark-checkpoint")

    //指定以后从哪里读取数据
    val lines=sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-03-TopN与WordCount执行过程详解\\课件与代码\\teacher.log")
    //整理数据
    val sbjectTeacherAndOne=lines.map(line=>{
      val teacherIndex=line.lastIndexOf("/")
      val host=new URL(line).getHost
      val sbject=host.split("\\.")(0)
      val teacher=line.substring(teacherIndex+1)
      ((sbject,teacher),1)
    })
//    val map=sbjectTeacherAndOne.map((_,1))
    //聚合，学科和老师联合当key
    val reduced: RDD[((String, String), Int)] =sbjectTeacherAndOne.reduceByKey(_+_)
    //cache到内存(标记为cache的rdd以后被反复使用，才使用cache)
    val cache=reduced.cache()
    reduced.checkpoint()
    //scala的集合排序在内存中进行，可能出现内存不够的情况
    //可以使用Rdd的sortBy方法，内存+磁盘进行排序
    for(sb <- subjects){
      //该rdd中对应的数据仅有一门学科的数据(因为已经过滤了)
     val filtered=cache.filter(_._1._1==sb)
     val favTeacher=filtered.sortBy(_._2,false).take(3)
      //打印
      println(favTeacher.toBuffer)
    }
    //
    sc.stop()
  }
}
