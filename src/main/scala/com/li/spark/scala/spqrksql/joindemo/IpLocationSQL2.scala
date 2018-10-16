package com.li.spark.scala.spqrksql.joindemo

import com.li.spark.scala.ipmapandcount.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object IpLocationSQL2 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("IpLocationSQL2").master("local").getOrCreate()

    //取到HDFS中的IP规则
    import spark.implicits._
    val rulesLines=spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\ip.txt")
    //整理IP规则数据
    val rulesDataset: Dataset[(Long, Long, String)] =rulesLines.map(line=>{
      val fields=line.split("[|]")
      val startNum=fields(2).toLong
      val endNum=fields(3).toLong
      val  province=fields(6)
      (startNum,endNum,province)
    })
    //IP规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] =rulesDataset.collect()
    //广播(必须使用sparkcontext)
    //将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] =spark.sparkContext.broadcast(rulesInDriver)
    //创建RDD，读取访问日志
    val accessLines =spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\access.log")
    //整理数据
    val ipDataFrame =accessLines.map(line=>{
      val fields=line.split("[|]")
      val ip=fields(1)
      //将IP转换为十进制
      val ipNum=MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipDataFrame.createTempView("v_log")

    //自定义一个函数(udf),并注册
    spark.udf.register("ip2Province",(ipNum:Long)=>{
      //查找IP规则(事先已经广播过，已经存在Executor中)
      //使用广播变量的引用，就可以获得
      val ipRulesInExecutor=broadcastRef.value
      //根据IP地址对应的十进制查找省份名称
      val index=MyUtils.binarySearch(ipRulesInExecutor,ipNum)
      var province="未知"
      if(index != -1){
        province=ipRulesInExecutor(index)._3
      }
      province
    })

   //执行SQL
    val result= spark.sql("select ip2Province(ip_num) province, count(*) counts from v_log group by province order by counts desc")
//    val result=spark.sql("select province,count(*) counts from v_ips join v_rules on ip_num >=startNum and ip_num <=endNum group by province")
    result.show()
    spark.stop()
  }
}
