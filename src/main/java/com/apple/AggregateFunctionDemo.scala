package com.apple

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * @Program: flink-scala
  * @ClassName: AggregateFunctionDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-11 23:23
  * @Version 1.1.0
  *         https://blog.csdn.net/slaron/article/details/107921465
  **/
object AggregateFunctionDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.addSource()
  }
}

class MySource
