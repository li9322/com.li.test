package com.li.spark.scala.ipmapandcount

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IpLocation1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val rules: Array[(Long, Long, String)] =MyUtils.readRules("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\ip.txt")
    //将driver端的数据广播到executor中

    //调用sc的广播方法
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] =sc.broadcast(rules)

//    val accessLines: RDD[String] =sc.textFile(args(1))
    val accessLines: RDD[String] =sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\access.log")
    //这个函数在哪一端定义的？(Driver)
    val func=(line:String)=>{
      val fields=line.split("[|]")
      val ip=fields(1)
      //将IP转换为十进制
      val ipNum=MyUtils.ip2Long(ip)
      //进行二分查找，通过Driver端的引用获取到Executor中的广播变量
      //（该函数中的代码是在Executor中分别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播变量
      val rulesInExecutor: Array[(Long, Long, String)] =broadcastRef.value
      //查找
      var province="未知"
      val index=MyUtils.binarySearch(rulesInExecutor,ipNum)
      if(index != -1){
        province=rulesInExecutor(index)._3
      }
      (province,1)
    }
    //整理数据
    val provinceAndOne: RDD[(String, Int)] =accessLines.map(func)
    //聚合
    //val sum=(x:Int,y:Int) => x + y
    val reduced=provinceAndOne.reduceByKey(_+_)
    //打印
    val result=reduced.collect()
    println(result.toBuffer)
    sc.stop()
  }
}
