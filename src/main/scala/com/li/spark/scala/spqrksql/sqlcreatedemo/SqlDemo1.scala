package com.li.spark.scala.spqrksql.sqlcreatedemo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SqlDemo1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    val lines=sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-06-spark SQL简介与案例详解1\\课件与代码\\person.txt")
    val boyRdd: RDD[Boy] =lines.map(line=>{
      val fields=line.split(",")
      val id =fields(0).toLong
      val name =fields(1)
      val age =fields(2).toInt
      val fv =fields(3).toDouble
      Boy(id,name,age,fv)
    })
    import sqlContext.implicits._
    val bdf=boyRdd.toDF
    bdf.registerTempTable("t_boy")
    val result: DataFrame =sqlContext.sql("select * from t_boy order by fv desc,age asc")
    result.show()
    sc.stop()
  }
}
case class Boy(id:Long,name:String,age:Int,fv:Double)