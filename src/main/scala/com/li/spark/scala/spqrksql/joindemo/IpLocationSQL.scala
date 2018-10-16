package com.li.spark.scala.spqrksql.joindemo

import java.sql.{Connection, DriverManager}

import com.li.spark.scala.ipmapandcount.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object IpLocationSQL {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("IpLocationSQL").master("local").getOrCreate()

    //将driver端的数据广播到executor中
    //取到HDFS中的IP规则
    import spark.implicits._
    val rulesLines: Dataset[String] =spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\ip.txt")
//    val ipRulesRDD: Dataset[(Long, Long, String)] =rulesLines.map(line=>{
    val rulesDataFrame: DataFrame =rulesLines.map(line=>{
      val fields=line.split("[|]")
      val startNum=fields(2).toLong
      val endNum=fields(3).toLong
      val  province=fields(6)
      (startNum,endNum,province)
    }).toDF("startNum","endNum","province")

    val accessLines: Dataset[String] =spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\access.log")
    //这个函数在哪一端定义的？(Driver)
    val func=(line:String)=>{
      val fields=line.split("[|]")
      val ip=fields(1)
      //将IP转换为十进制
      val ipNum=MyUtils.ip2Long(ip)
      ipNum
    }
    //整理数据
    val ipDataFrame: DataFrame =accessLines.map(func).toDF("ip_num")

    rulesDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")
    val result=spark.sql("select province,count(*) counts from v_ips join v_rules on ip_num >=startNum and ip_num <=endNum group by province")
    result.show()
    spark.stop()
  }
}
