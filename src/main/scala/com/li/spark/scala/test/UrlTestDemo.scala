package com.li.spark.scala.test

import java.net.URL

object UrlTestDemo {
  def main(args: Array[String]): Unit = {
    val url = "http://bigdata.edu360.cn/laozhao"
    val teacherIndex=url.lastIndexOf("/")
    val host=new URL(url).getHost
    val course=host.split("\\.")(0)
    val teacher=url.substring(teacherIndex+1)
    println(host)
    println(course)
    println(teacher)
  }
}
