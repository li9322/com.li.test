package com.li.spark.scala.spqrksql.sqlcreatedemo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SqlDemo1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    val lines=sc.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-06-spark SQL简介与案例详解1\\课件与代码\\person.txt")
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
   val bdf: DataFrame = sqlContext.createDataFrame(rowRdd,schema)
   val df1: DataFrame = bdf.select("name","age","fv")
    import sqlContext.implicits._
   val df2: Dataset[Row] = df1.orderBy($"fv" desc,$"age" asc)
//    val result: DataFrame =sqlContext.sql("select * from t_boy order by fv desc,age asc")
    df2.show()
    sc.stop()
  }
}
