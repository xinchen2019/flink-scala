package com.apple.Demo

import org.apache.flink.streaming.api.scala._

/**
  * @Program: flink-scala
  * @ClassName: Demo02
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-16 11:41
  * @Version 1.1.0
  **/
object Demo02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "C:\\Users\\13128\\Downloads\\redis.conf"
    val stream2 = env.readTextFile(inputPath)
      .filter(!_.contains("#")).filter(_.nonEmpty)

    stream2.print()
    env.execute()
  }
}
