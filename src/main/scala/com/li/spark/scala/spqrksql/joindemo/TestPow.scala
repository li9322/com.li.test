package com.li.spark.scala.spqrksql.joindemo

object TestPow {
  def main(args: Array[String]): Unit = {
    val i=3
    val r=Math.pow(i,1.toDouble/2)
    println(r)
  }
  def fun(i:Int):Int={
   if (i==0) 1 else i* fun(i-1)
  }
}
