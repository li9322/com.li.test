package com.li.spark.scala.teache2topn

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc=new SparkContext(conf)
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
    val reduced: RDD[((String, String), Int)] =sbjectTeacherAndOne.reduceByKey(_+_,numPartitions = 1)
    //分组排序(按学科进行排序)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] =reduced.groupBy(_._1._1,numPartitions = 1)
    //经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    //将每一个组拿出来进行操作
    //为什么可以调用sacla的sortby方法呢？因为一个学科的数据已经在一台机器上的一个scala集合里面了
    val sorted: RDD[(String, List[((String, String), Int)])] =grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    val result=sorted.collect()
    //打印
    println(result.toBuffer)
    sc.stop()
  }
}
