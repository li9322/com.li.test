package com.li.spark.scala.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSrot6 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IpLocation1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines: RDD[String] =sc.parallelize(users)
    val tpRDD: RDD[(String, Int, Int)] =lines.map(line=>{
      val fields=line.split(" ")
      val name=fields(0)
      val age=fields(1).toInt
      val fv=fields(2).toInt
      (name,age,fv)
    })
    //利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
    implicit val rules=Ordering[(Int,Int)].on[(String,Int,Int)](t=>(-t._3,t._2))
    val sorted=tpRDD.sortBy(tp=>(-tp._3,tp._2))
    println(sorted.collect().toBuffer)
    sc.stop()
  }
}
