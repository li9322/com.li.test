package com.li.spark.scala.akka.rpcdemo

class WorkerInfo(val id: String, val memory: Int, val cores: Int) {
  //TODO 上一次心跳
  var lastHeartbeatTime : Long = _
}
