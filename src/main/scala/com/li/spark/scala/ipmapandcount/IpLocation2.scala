package com.li.spark.scala.ipmapandcount

import java.sql.{Connection, DriverManager}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IpLocation1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //将driver端的数据广播到executor中
    //取到HDFS中的IP规则
    val rulesLines=sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\ip\\ip.txt")
    val ipRulesRDD: RDD[(Long, Long, String)] =rulesLines.map(line=>{
      val fields=line.split("[|]")
      val startNum=fields(2).toLong
      val endNum=fields(3).toLong
      val  province=fields(6)
      (startNum,endNum,province)
    })

    //
    val rulesInDriver: Array[(Long, Long, String)] =ipRulesRDD.collect()
    //将Driver端的数据广播到executor端
    //广播变量的引用(还在driver端)
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] =sc.broadcast(rulesInDriver)

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
//    val result=reduced.collect()
//    println(result.toBuffer)

    /**
    reduced.foreach(tp=>{
      //将数据写入到MySQL中
      //注意：在executor中的task获取的jdbc连接
      val conn: Connection =DriverManager.getConnection("jdbc","root","liBAO@12")
      //写入大量数据的时候，存在问题
      val pstm=conn.prepareStatement("")
      pstm.setString(1,tp._1)
      pstm.setInt(2,tp._2)
      pstm.executeUpdate()
      pstm.close()
      conn.close()
    })
      */
    //一次拿一个分区的连接，可以将一个分区中的多条数据写完再释放jdbc连接，这样节省资源
//    reduced.foreachPartition(it=>{
//      val conn: Connection =DriverManager.getConnection("jdbc:mysql://192.168.70.10:3306/bigdata?characterEncoding=UTF-8","root","liBAO@12")
//      val pstm=conn.prepareStatement("insert into access_log values (?, ?)")
//      //将一个分区中的每一条数据拿出来
//      it.foreach(tp=>{
//        pstm.setString(1,tp._1)
//        pstm.setInt(2,tp._2)
//        pstm.executeUpdate()
//      })
//      pstm.close()
//      conn.close()
//    })

    reduced.foreachPartition(it =>MyUtils.data2MySQL(it))

    sc.stop()
  }
}
