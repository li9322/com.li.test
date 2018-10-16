package com.li.spark.scala.wordcount

import com.li.spark.scala.sort.{SortRules}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BaseSparkDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IpLocation1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    //名字 语文 数学
    //排序规则：首先按照数学降序，如果数学相等，再按照语文的升序
    val users= Array("xiaoming 80 99", "xiaozhang 39 99", "xiaoliang 28 97", "xiaoma 88 77")
    val lines: RDD[String] =sc.parallelize(users)
    val userRDD: RDD[(String, Int, Int)] =lines.map(line=>{
      val fields=line.split(" ")
      val name=fields(0)
      val yuwen=fields(1).toInt
      val math=fields(2).toInt
      (name,yuwen,math)
    })
    //排序(传入一个排序规则，不会改编数据的格式，只会改变顺序)
    val sorted=userRDD.sortBy(tp=> xuesheng(tp._2,tp._3))
    println(sorted.collect().toBuffer)
    sc.stop()
  }
}

case class xuesheng(yuwen:Int,math:Int) extends Ordered[xuesheng] with Serializable {
  override def compare(that: xuesheng): Int = {
    if(this.yuwen == that.yuwen){
      this.math - that.math
    }else{
      - (this.yuwen - that.yuwen)
    }
  }
}