package com.li.spark.scala.spqrksql.udf


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

/**
  * 实现UDAF函数如果要自定义类要继承UserDefinedAggregateFunction类
  */
object Udafdemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("udaf")
      .master("local[2]")
      //      .enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法)
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    val rdd=sc.makeRDD(Array("zhangsan","lisi","wangwu","zhangsan","zhangsan","lisi"))
    val rddRow: RDD[Row] =rdd.map(x=>{ RowFactory.create(x)})
    val schema =DataTypes.createStructType(Array(StructField("name",StringType,true)))
    val df=spark.createDataFrame(rddRow,schema)
    df.registerTempTable("user")
    spark.udf.register("StringCount",new UserDefinedAggregateFunction {
      //输入数据的类型
      override def inputSchema: StructType = StructType((List(
        StructField("value",DoubleType)
      )))
      /**
        * 在进行聚合操作的时候所要处理的数据的结果的类型
        */
      //产生中间结果的数据类型
      override def bufferSchema: StructType = StructType((List(
      
      )))
      //最终返回的结果类型
      override def dataType: DataType =DoubleType
      //确保一致性 一般用true
      override def deterministic: Boolean = true
      //指定初始值,在Aggregate之前每组数据的初始化结果
      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0,0)
      }
      /**
        * 更新 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
        * buffer.getInt(0)获取的是上一次聚合后的值
        * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合
        * 大聚和发生在reduce端.
        * 这里即是:在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
        */
      //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer.update(0, buffer.getInt(0)+1)
      }
      /**
        * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
        * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
        * buffer1.getInt(0) : 大聚合的时候 上一次聚合后的值
        * buffer2.getInt(0) : 这次计算传入进来的update的结果
        * 这里即是：最后在分布式节点完成后需要进行全局级别的Merge操作
        * 也可以是一个节点里面的多个executor合并
        */
      //全局聚合
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
      }
      //计算最终的结果
      override def evaluate(buffer: Row): Double = {
        math.pow(buffer.getDouble(1),1.toDouble/buffer.getLong(0))
      }
    })

  }
}
