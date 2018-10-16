package com.li.spark.scala.ipmapandcount

import org.apache.spark.{SparkConf, SparkContext}

object IPSplit {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Ip").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\access.log")
    val ipAndOne=lines.map(line=>{
      val ipFile=line.split("[|]")
      val ip=ipFile(1)
      val ipNumb=ip2Long(ip)
      println(ipNumb)
      (ipNumb,1)
    })
    val reduceed=ipAndOne.reduceByKey(_+_)
    val sorted=reduceed.sortBy(_._1,false)
    //触发Action执行计算
    val result=sorted.collect().take(3)
    //打印
    println(result.toBuffer)
//    sorted.saveAsTextFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\out")
    sc.stop()
  }

def ip2Long(ip: String): Long = {
  val fragments = ip.split("[.]")
  var ipNum = 0L
  for (i <- 0 until fragments.length){
    ipNum =  fragments(i).toLong | ipNum << 8L
  }
  ipNum
}




}
