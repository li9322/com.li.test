package com.li.spark.scala.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSrot3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IpLocation1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines: RDD[String] =sc.parallelize(users)
    val userRDD: RDD[(String, Int, Int)] =lines.map(line=>{
      val fields=line.split(" ")
      val name=fields(0)
      val age=fields(1).toInt
      val fv=fields(2).toInt
      (name,age,fv)
    })
    //排序(传入一个排序规则，不会改编数据的格式，只会改变顺序)
    val sorted=userRDD.sortBy(tp=> Man(tp._2,tp._3))
    println(sorted.collect().toBuffer)
    sc.stop()
  }
}
case class Man(age:Int,fv:Int) extends Ordered[Man] with Serializable {
   override def compare(that: Man): Int = {
     if(this.fv == that.fv){
       this.age - that.age
     }else{
       - (this.fv - that.fv)
     }
   }
 }
