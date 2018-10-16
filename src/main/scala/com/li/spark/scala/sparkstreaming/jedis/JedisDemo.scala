package com.li.spark.scala.sparkstreaming.jedis

import redis.clients.jedis.Jedis

object JedisDemo{
  def main(args: Array[String]): Unit = {
    val jedis=new Jedis("47.95.207.79",6379)
//    val conn=jedis.getRe
//    conn.get
  }
}
