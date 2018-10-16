package com.li.spark.scala.teache2topn

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FavTeacher").setMaster("local")
    val sc=new SparkContext(conf)
//    val subjects = Array("bigdata", "javaee", "php")
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
    //计算有多少学科
    val subjects: Array[String] =reduced.map(_._1._1).distinct().collect()
    //自定义一个分区器，按指定的分区器进行分区
    val sbPartitioner=new SubjectPartitioner(subjects)
    //partitionBy按照指定的分区规则进行分区
   val partitionere=reduced.partitionBy(sbPartitioner)
    //如果一次拿出一个分区(可以操作一个分区的数据)
   val sorted= partitionere.mapPartitions(it=>{
     //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).take(3).iterator
    })
    val favTeacher=sorted.collect()
      //打印
      println(favTeacher.toBuffer)

    sc.stop()
  }
}
class SubjectPartitioner(sbs:Array[String]) extends Partitioner{
  //相当于主构造器，(new 的时候执行一次)
  //用于存放规则的一个map
  var i=0
  val rules=new mutable.HashMap[String,Int]()
  for(sb<-sbs){
    rules.put(sb,i)
    i+=1
  }

  //返回分区的数量(下一个rdd有多少分区)
  override def numPartitions: Int = sbs.length
  //根据传入的key计算分区标号
  //key是一个元组(String,String)
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject=key.asInstanceOf[(String,String)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}