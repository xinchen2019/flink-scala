package com.apple.udf

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * @Program: flink-scala
  * @ClassName: AggregateFunctionDemo
  * @Description: 自定义聚合函数AggregateFunction
  * @Author Mr.Apple
  * @Create: 2021-09-11 23:23
  * @Version 1.1.0
  *          https://blog.csdn.net/slaron/article/details/107921465、
  *          需求：有一天小白想统计一下每5次各科成绩总和以及平均值
  **/
object AggregateFunctionDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "data\\sscore.txt"
    val stream2 = env.readTextFile(inputPath).map {
      line =>
        val arr = line.split(",")
        (arr(0), arr(1), arr(2).toInt: Integer)
    }
    val score = stream2.keyBy(_._1)
      .countWindow(5)
      .aggregate(new AverageAggrate())
    score.print()
    //val source = env.addSource(new MySourceFunction2())
    //    val score = source.keyBy(_._1)
    //      .countWindow(5)
    //      .aggregate(new AverageAggrate())
    //
    //    score.print()
    // source.print()
    env.execute("AggregateFunctionDemo")
  }
}

class AverageAggrate extends AggregateFunction[
  Tuple3[String, String, Integer],
  Tuple4[String, String, Integer, Integer],
  Tuple4[String, String, Integer, Double]] {
  //创建一个新的累加器，启动一个新的聚合,负责迭代状态的初始化
  override def createAccumulator(): (String, String, Integer, Integer) = {
    println("createAccumulator: ")
    return new Tuple4[String, String, Integer, Integer]("", "", 0, 0)
  }

  //对于数据的每条数据，和迭代数据的聚合的具体实现
  override def add(value: (String, String, Integer), acc: (String, String, Integer, Integer)): (String, String, Integer, Integer) = {
    println("add: ")
    return new Tuple4[String, String, Integer, Integer](value._1, value._2, acc._3 + value._3, acc._4 + 1)
  }

  //从累加器获取聚合的结果
  override def getResult(acc: (String, String, Integer, Integer)): (String, String, Integer, Double) = {
    println("getResult: ")
    return new Tuple4[String, String, Integer, Double](acc._1, acc._2, acc._3, acc._3.toDouble / acc._4)
  }

  //合并两个累加器，返回一个具有合并状态的累加器
  override def merge(a: (String, String, Integer, Integer), b: (String, String, Integer, Integer)): (String, String, Integer, Integer) = {
    println("merge: ")
    return null
  }
}

class MySourceFunction2 extends SourceFunction[(String, String, Integer)] {
  val subject = Array("语文", "数学", "英语", "物理", "化学")
  val person = Array("小红", "小白", "小黑")
  println("==============")
  private val random = new Random()
  @volatile private var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[(String, String, Integer)]): Unit = {
    while (isRunning) {
      ctx.collect(
        (
          person(random.nextInt(person.length)),
          subject(random.nextInt(subject.length)),
          (Math.random() * 50).toInt + 50)
      )
    }
  }

  override def cancel() {
    isRunning = false
  }
}
