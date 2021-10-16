package com.apple.udf

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._

/**
  * @Program: flink-scala
  * @ClassName: AggregateFunctionDemo2
  * @Description: 自定义聚合函数AggregateFunction
  * @Author Mr.Apple
  * @Create: 2021-09-11 23:23
  * @Version 1.1.0
  *          https://www.cnblogs.com/linjiqin/p/12591710.html
  **/
object AggregateFunctionDemo2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    /**
      * "class1,张三,100",
      * "class1,李四,60",
      */
    val input = Array(
      "class1,张三,100",
      "class1,李四,60",
      "class1,老六,60",
      "class2,王二,20",
      "class2,麻子,30",
      "class2,赵四,50"
    )
    val avgScore = env.fromCollection(input)
      .map { line =>
        val arr = line.split(",")
        (arr(0), arr(1), arr(2).toLong)
      }.keyBy(_._1)
      .countWindow(3)
      .aggregate(new AverageAggrate2)
    avgScore.print("avg")
    env.execute("AggregateFunctionDemo2")
  }
}

class AverageAggrate2 extends AggregateFunction[Tuple3[String, String, Long], Tuple2[Long, Long], Double] {
  /**
    * 创建累加器保存中间状态(sum,count)
    *
    * @return
    * sum英语总成绩
    * count 学生个数
    */
  override def createAccumulator(): (Long, Long) = {
    return new Tuple2[Long, Long](0L, 0L)
  }

  /**
    * 将元素添加到累加器并返回新的累加器
    *
    * @param value
    * @param acc
    * @return
    */
  override def add(value: (String, String, Long), acc: (Long, Long)): (Long, Long) = {
    return new Tuple2[Long, Long](value._3 + acc._1, acc._2 + 1L)
  }

  /**
    * 从累加器提取结果
    *
    * @param acc
    * @return
    */
  override def getResult(acc: (Long, Long)): Double = {
    return acc._1.asInstanceOf[Double] / acc._2
  }

  /**
    * 累加器合并
    *
    * @param a
    * @param b
    * @return
    */
  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    return (a._1 + b._1, a._2 + b._2)
  }
}

