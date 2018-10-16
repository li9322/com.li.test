package com.li.spark.scala.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSrot1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IpLocation1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
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

    //不满足要求
    //tpRDD.sortBy(tp=>tp._3,false)
    //将RDD里面的User类型数据进行排序
    val sorted: RDD[(String, Int, Int)] =userRDD.sortBy(u=>u)
    val result=sorted.collect()
    println(result.toBuffer)
  }
}
 class User(val name:String,val age:Int,val fv:Int) extends Ordered[User] with Serializable {
   override def compare(that: User): Int = {
     if(this.fv == that.fv){
       this.age - that.age
     }else{
       - (this.fv - that.fv)
     }
   }
   override def toString:String=s"name:$name,age:$age,fv:$fv"
 }