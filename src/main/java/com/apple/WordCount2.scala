package com.apple

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * @Program: flink-scala
  * @ClassName: WordCount2
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-12 16:20
  * @Version 1.1.0
  **/
object WordCount2 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = Array(
      "class1,张三,100",
      "class1,李四,60",
      "class1,老六,60",
      "class2,王二,20",
      "class2,麻子,30",
      "class2,赵四,50"
    )
    val a = env.fromCollection(input)
      .map { line =>
        val arr = line.split(",")
        (arr(0), arr(1), arr(2).toLong)
      }.groupBy(0)
      .aggregate(Aggregations.SUM, 2)
    a.print()
  }
}
