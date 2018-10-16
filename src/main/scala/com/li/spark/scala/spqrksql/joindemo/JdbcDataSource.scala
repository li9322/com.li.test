package com.li.spark.scala.spqrksql.joindemo

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("JdbcDataSource").master("local").getOrCreate()
    import spark.implicits._

    val logs: DataFrame =spark.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://47.95.207.79:3306/sparktest",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"logs",
      "user"->"root",
      "password"->"root")
      ).load()
//    logs.printSchema()

//    logs.show()
//    val filter: Dataset[Row] =logs.filter(r => {
////      r.getAs[Int](2) <= 25
//      r.getAs[Int]("age") <= 25
//    })
    //lambda表达式
    val filter: Dataset[Row] =logs.filter($"age"<=25)

//    val filter: Dataset[Row] =logs.where($"age"<=25)
//    filter.show()

//    val reslut: DataFrame =filter.select($"id",$"name",$"age" * 2 as "new_age")
    val reslut: DataFrame =filter.select($"name")

//    val props=new Properties()
//    props.put("user","root")
//    props.put("password","root")
//    reslut.write.mode("ignore").jdbc("jdbc:mysql://47.95.207.79:3306/sparktest","logs1",props)
//      reslut.write.text("D:\\text1\\1.txt")
//      reslut.write.text("D:\\text1\\json")
      reslut.write.text("D:\\text1\\csv")
//    reslut.show()

    spark.close()
  }
}
