package com.li.spark.scala.spqrksql.sqlcreatedemo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlTest1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("SqlTest1")
      .master("local[2]")
      .getOrCreate()
    val lines=spark.sparkContext.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-06-spark SQL简介与案例详解1\\课件与代码\\person.txt")
    val rowRdd: RDD[Row] =lines.map(line=>{
      val fields=line.split(",")
      val id =fields(0).toLong
      val name =fields(1)
      val age =fields(2).toInt
      val fv =fields(3).toDouble
      Row(id,name,age,fv)
    })
    val schema: StructType =StructType(List(
      StructField("id",LongType, true),
      StructField("name",StringType, true),
      StructField("age",IntegerType, true),
      StructField("fv",DoubleType, true)
    ))
    val df: DataFrame =spark.createDataFrame(rowRdd,schema)
    import spark.implicits._
//    df.filter($"name")
    val df1=df.where($"fv">98).orderBy($"fv" desc,$"age" asc)
    df1.show()
    spark.stop()
  }
}
