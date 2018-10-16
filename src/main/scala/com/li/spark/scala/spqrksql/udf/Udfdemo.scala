package com.li.spark.scala.spqrksql.udf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StringType, StructField}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

/**
  * SparkSQL中的UDF相当于是1进1出，UDAF相当于是多进一出，类似于聚合函数。
  * 开窗函数一般分组取topn时常用。
 */
object Udfdemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("udf")
      .master("local[2]")
//      .enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法)
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    val rdd: RDD[String] =sc.makeRDD(Array("zhansan","lisi","wangwu"))
    val rddRow: RDD[Row] =rdd.map(x=>{RowFactory.create(x)})
    val schema =DataTypes.createStructType(Array(StructField("name",StringType,true)))
    val df=spark.createDataFrame(rddRow,schema)
    df.registerTempTable("user")
    spark.udf.register("StrLen",(s : String,i:Int)=>{s.length()+i})
    spark.sql("select name ,StrLen(name,10) as length from user").show
    spark.stop()

  }
}
