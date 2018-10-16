package com.li.spark.scala.spqrksql.joindemo

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLFavTeacher {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("JoinTest").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //指定以后从哪里读取数据
    val lines=spark.read.textFile("F:\\小牛大数据\\06-Spark安装部署到高级-10天\\spark-03-TopN与WordCount执行过程详解\\课件与代码\\teacher.log")

    import spark.implicits._
    val df: DataFrame =lines.map(line=>{
      val teacherIndex=line.lastIndexOf("/")+1
      val host=new URL(line).getHost
      val subject=host.split("[.]")(0)
      val teacher=line.substring(teacherIndex)
      (subject,teacher)
    }).toDF("subject","teacher")

    df.createTempView("v_sub_teacher")

    //该学科下老师的访问次数
    val temp1=spark.sql("select subject,teacher,count(*) counts from v_sub_teacher group by subject,teacher")
    val topN=2
    //求每个学科下最受欢迎的老师topn
    temp1.createTempView("v_temp_sub_teacher_counts")
//    val temp2=spark.sql("select subject,teacher,counts,row_number() over(partition by subject order by counts desc) rk from v_temp_sub_teacher_counts")
//    val temp2=spark.sql("select subject,teacher,counts,row_number() over(partition by subject order by counts desc) sub_rk,row_number() over(order by counts desc) g_rk from v_temp_sub_teacher_counts")
//    val temp2=spark.sql(s"select * from (select subject,teacher,counts,row_number() over(partition by subject order by counts desc) sub_rk,rank() over(order by counts desc) g_rk from v_temp_sub_teacher_counts) tmp where sub_rk <= $topN")
//    val temp2=spark.sql(s"select *,row_number() over(order by counts desc) g_rk from (select subject,teacher,counts,row_number() over(partition by subject order by counts desc) sub_rk from v_temp_sub_teacher_counts) tmp where sub_rk <= $topN")
    val temp2=spark.sql(s"select *,dense_rank() over(order by counts desc) g_rk from (select subject,teacher,counts,row_number() over(partition by subject order by counts desc) sub_rk from v_temp_sub_teacher_counts) tmp where sub_rk <= $topN")

    temp2.show()

    spark.stop()
  }
}
