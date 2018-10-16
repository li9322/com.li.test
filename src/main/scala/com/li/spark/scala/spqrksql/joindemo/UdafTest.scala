package com.li.spark.scala.spqrksql.joindemo

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object UdafTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("UdafTest").master("local").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    val geomean=new GeoMean

    val range: Dataset[lang.Long] =spark.range(1,11)


    //注册函数
//    spark.udf.register("gm",geomean)
    //将range这个dataSet[]
//    range.createTempView("v_range")
//    val result=spark.sql("select gm(id) result from v_range")

    import spark.implicits._
    val result: DataFrame =range.groupBy().agg(geomean($"id").as("geomean"))

    result.show()
    spark.stop()
  }
}

class GeoMean extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = StructType((List(
    StructField("value",DoubleType)
  )))
  //产生中间结果的数据类型
  override def bufferSchema: StructType = StructType((List(
    //参与运算的数字的个数
    StructField("counts",LongType),
    //相乘之后返回的基
    StructField("product",DoubleType)
  )))
  //最终返回的结果类型
  override def dataType: DataType =DoubleType
  //确保一致性 一般用true
  override def deterministic: Boolean = true
  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //参与运算数字的个数
    buffer(0)=0L
    //参与相乘的初始值
    buffer(1)=1.0

  }
  //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每有一条数字参与运算就进行相乘(包含中间结果)
    buffer(1)=buffer.getDouble(1)*input.getDouble(0)
    //参与运算的数字也进行更新
    buffer(0)=buffer.getLong(0) + 1L
  }
  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区参与运算就进行相乘(包含中间结果)
    buffer1(1)=buffer1.getDouble(1)*buffer2.getDouble(1)
    //每个分区参与运算的个数进行相加
    buffer1(0)=buffer1.getLong(0) + buffer2.getLong(0)
  }
  //计算最终的结果
  override def evaluate(buffer: Row): Double = {
    math.pow(buffer.getDouble(1),1.toDouble/buffer.getLong(0))
  }
}