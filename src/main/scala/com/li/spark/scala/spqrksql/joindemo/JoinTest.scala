package com.li.spark.scala.spqrksql.joindemo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("JoinTest").master("local").getOrCreate()
    import spark.implicits._
    val lines: Dataset[String] =spark.createDataset(List("1,laozhao,china","2,laoduan,usa"))
    val tpDs=lines.map(line=>{
      val fields=line.split(",")
      val id=fields(0).toLong
      val name=fields(1)
      val nationCode=fields(2)
      (id,name,nationCode)
    })
    val df1: DataFrame =tpDs.toDF("id","name","nation")
    val nations: Dataset[String] =spark.createDataset(List("china,中国","usa,美国"))
    val ndataset=nations.map(l=>{
      val fields=l.split(",")
      val ename=fields(0)
      val cname=fields(1)
      (ename,cname)
    })
    val df2: DataFrame =ndataset.toDF("ename","cname")
    //创建视图
//    df1.createTempView("v_users")
//    df2.createTempView("v_nations")
//    val result=spark.sql("select name,cname from v_users join v_nations on nation=ename")

    val result: DataFrame = df1.join(df2,$"nation"===$"ename")

    val testdf1=Seq(
      (0,"playing"),
      (1,"with"),
      (2,"join")
    ).toDF("id","token")
    val testdf2=Seq(
      (0,"P"),
      (1,"W"),
      (2,"S")
    ).toDF("aid","atoken")
    testdf1.join(testdf2,$"id"===$"aid")

    result.show()
    spark.stop()
  }
}
