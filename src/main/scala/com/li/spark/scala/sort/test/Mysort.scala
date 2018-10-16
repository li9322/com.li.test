package com.li.spark.scala.sort.test

import org.apache.spark.{SparkConf, SparkContext}

object Mysort {
//  1.基础排序算法
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Mysort").setMaster("local[2]")
    val sc=new SparkContext(conf)
    //ni hao/ ni da ye /ni ma/ ni hao ma
    val path="D:\\test\\test.txt"



    oneSort(sc,path)
  }

  //所谓二次排序就是指排序的时候考虑两个维度（有可能10次排序）
  def oneSort(sc:SparkContext,path:String){
    val rdd=sc.textFile(path).flatMap(_.split(" ")).map(word=>(word,1))
      .reduceByKey(_+_,1)//ArrayBuffer((2,ma), (1,da), (1,ye), (4,ni), (2,hao))
      .map(pair=>(pair._2,pair._1))
      .sortByKey(false)//ArrayBuffer((4,ni), (2,ma), (2,hao), (1,da), (1,ye))
      .map(pair=>(pair._2,pair._1))
      .collect
    //key value交换
    sc.setLogLevel("WARN")
    println(rdd.toBuffer)
  }
  //所谓二次排序就是指排序的时候考虑两个维度（有可能10次排序）
  def twoSort(sc:SparkContext,path:String): Unit ={
    val lines = sc.textFile(path)

    val pairWithSortkey = lines.map(line =>(
      new SecondarySort( line.split(" ")(0).toInt,line.split(" ")(1).toInt),line
    ))
    val sorted = pairWithSortkey.sortByKey(false)
    val sortedResult = sorted.map(sortedline => sortedline._2)
    sortedResult.collect.foreach(println)
  }
  class SecondarySort(val first:Int, val second:Int) extends Ordered[SecondarySort] with Serializable{
    override def compare(that: SecondarySort): Int = {
      if(this.first - that.first != 0)
      {
        this.first - that.first
      }else {
        this.second - that.second
      }

    }
  }
}
