package com.apple

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Program: flink-scala
  * @ClassName: TransformTest
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-09 10:49
  * @Version 1.1.0
  **/
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val inputPath = "data//sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream.map { line =>
      val arr = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }
    //分组聚合，输出每个传感器当前最小值
    val aggStream =
      dataStream.keyBy("id")
        .minBy("temperature")

    aggStream.print()
    inputStream.print()


    env.execute("TransformTest")
  }
}
