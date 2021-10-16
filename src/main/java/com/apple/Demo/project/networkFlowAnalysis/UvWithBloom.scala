package com.apple.Demo.project.networkFlowAnalysis

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Program: flink-scala
  * @ClassName: UvWithBloom
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-15 18:04
  * @Version 1.1.0
  **/
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

  }
}
