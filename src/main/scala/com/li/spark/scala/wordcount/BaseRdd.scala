package com.li.spark.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object BaseRdd {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("wordcont").setMaster("local[2]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //transformation
//    mapTest(sc)
//    filterTest(sc)
//    groupByKeyTest(sc)
//    reduceByKeyTest(sc)
//    sortByKeyTest(sc)
//    joinTest(sc)
//    cogroupTest(sc)
    //action
//    reduceTest(sc)
//    collectTest(sc)
//    takeTest(sc)
    countByKeyTest(sc)
    sc.stop()
  }
  def mapTest(sc: SparkContext): Unit ={
    val numbers = Array(1, 2, 3, 4, 5)
    val numberRDD = sc.parallelize(numbers, 1)
    val multipleNumberRDD = numberRDD.map { num => num * 2 }

    multipleNumberRDD.foreach { num => println(num) }
  }
  def flatMapTest(sc: SparkContext): Unit ={
    val lineArray = Array("hello you", "hello me", "hello world")
    val lines = sc.parallelize(lineArray, 1)
    val words = lines.flatMap { line => line.split(" ") }

    words.foreach { word => println(word) }
  }
  def filterTest(sc: SparkContext): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 1)
    val evenNumberRDD = numberRDD.filter { num => num % 2 == 0 }

    evenNumberRDD.foreach { num => println(num)
    }
  }

  /**
    * 4.gropuByKey：根据key进行分组，每个key对应一个Iterable。
    * 案例：按照班级对成绩进行分组
    * @param sc
    */
  def groupByKeyTest(sc: SparkContext): Unit = {
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75), Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val groupedScores = scores.groupByKey()

    groupedScores.foreach(score => {
      println(score._1);
      score._2.foreach { singleScore => println(singleScore) };
      println("=============================")
    })
  }
  /**
    * 5.reduceByKey：对每个key对应的value进行reduce操作。
    * 案例：统计每个班级的总分
    * @param sc
    */
  def reduceByKeyTest(sc: SparkContext): Unit = {
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75), Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val totalScores = scores.reduceByKey(_ + _)

    totalScores.foreach(classScore => println(classScore._1 + ": " + classScore._2))
  }
  /**
    * 6.sortByKey：对每个key对应的value进行排序操作。
    * 案例：按照学生分数进行排序
    * @param sc
    */
  def sortByKeyTest(sc: SparkContext): Unit = {
    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"), Tuple2(100, "marry"), Tuple2(85, "jack"))
    val scores = sc.parallelize(scoreList, 1)
    val sortedScores = scores.sortByKey(false)

    sortedScores.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))
  }
  /**
    * 7.join:对两个包含<key,value>对的RDD进行join操作，每个key join上的pair，都会传入自定义函数进行处理。
    * 案例：打印学生成绩
    * @param sc
    */
  def joinTest(sc: SparkContext): Unit = {
    val studentList = Array(Tuple2(1, "leo"), Tuple2(2, "jack"), Tuple2(3, "tom"))
    val scoreList = Array(Tuple2(1, 100),Tuple2(1, 50), Tuple2(2, 90), Tuple2(3, 60))

    val students = sc.parallelize(studentList);
    val scores = sc.parallelize(scoreList);

    val studentScores = students.join(scores)
    println(studentScores.collect().toBuffer)
    println("------------------------------------")
    studentScores.foreach(studentScore => {
      println("student id: " + studentScore._1);
      println("student name: " + studentScore._2._1)
      println("student socre: " + studentScore._2._2)
      println("=======================================")
    })
  }
  /**
    * 8.cogroup：同join，但是是每个key对应的Iterable都会传入自定义函数进行处理。
    * 案例：打印学生成绩
    * @param sc
    */
  def cogroupTest(sc: SparkContext): Unit = {
    val studentList = Array(Tuple2(1, "leo"), Tuple2(2, "jack"), Tuple2(3, "tom"))
    val scoreList = Array(Tuple2(1, 100),Tuple2(1, 70), Tuple2(2, 90),Tuple2(2, 80), Tuple2(3, 60),Tuple2(3, 50))

    val students = sc.parallelize(studentList);
    val scores = sc.parallelize(scoreList);

    val studentScores = students.cogroup(scores)
    println(studentScores.collect().toBuffer)
//    println("------------------------------------")
    studentScores.foreach(studentScore => {
      println("student id: " + studentScore._1);
      println("student name: " + studentScore._2._1)
      println("student socre: " + studentScore._2._2)
      println("=======================================")
    })
  }
  //------------------------------------------------------------------
  /**
    *1.reduce：将RDD中的所有元素进行聚合操作。第一个和第二个元素聚合，值与第三个元素聚合，值与第四个元素聚合，以此类推。
   */
  def reduceTest(sc: SparkContext): Unit ={
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray, 1)
    val sum = numbers.reduce(_ + _)
    println(sum.toString)
  }
  /**
    *2.collect：将RDD中所有元素获取到本地客户端。
    * 3.count：获取RDD元素总数。
    * 4.saveAsTextFile：将RDD元素保存到文件中，对每个元素调用toString方法。
    * 5.take：获取RDD中前n个元素。
    */
  def collectTest(sc: SparkContext): Unit ={
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbers = sc.parallelize(numberArray, 1)
    numbers.count()
    println(numbers.count())
    val top3Number=numbers.take(3)//取出来是一个迭代器
    println(top3Number.toBuffer)
    println("----------------------")

    val numbersArray=numbers.collect()
    println(numberArray.toBuffer)
    for (num <- numbersArray){
      println(num)
    }
  }

  /**
    * 提取RDD的第一个N项并将它们作为数组返回。（注意：这听起来非常简单，但是对于Spark的实现者来说，
    * * 这实际上是一个相当棘手的问题，因为所讨论的项可以在许多不同的分区中）。
    * @param sc
    */
  def takeTest(sc: SparkContext): Unit ={
    val b1 = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
    val b11=b1.take(2)
    println(b11.toBuffer)
    val b2 = sc.parallelize(1 to 10000, 5000)
    val b12=b2.take(100)
    println(b12.toBuffer)
  }

  /**
    *6.countByKey：对每个key对应的值进行count计数。
    */
  def countByKeyTest(sc: SparkContext): Unit ={
    val studentList = Array(Tuple2("class1", "leo"), Tuple2("class2", "jack"),
      Tuple2("class1", "tom"), Tuple2("class2", "jen"), Tuple2("class2", "marry"))
    val students = sc.parallelize(studentList, 1)
    val studentCounts = students.countByKey()
//    for (studet <- studentCounts){
//      println(studet)
//    }
    studentCounts.foreach(clazz=>{println(clazz._1+"::"+clazz._2)})
  }
}
