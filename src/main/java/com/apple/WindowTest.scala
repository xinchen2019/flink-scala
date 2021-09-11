package com.apple

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Program: flink-scala
  * @ClassName: WindowTest
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-09 17:52
  * @Version 1.1.0
  **/
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    // 读取数据
    val inputPath = "data\\sensor.txt"
    //val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("master", 9999)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val resultStream = dataStream
      .map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1)
      //.window(TumblingEventTimeWindows.of(Time.seconds(15))) //滑动窗口
      //.window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3))) //滑动窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(10))) //会话窗口
      // .timeWindow(Time.seconds(10))
      /// .timeWindow(Time.seconds(15))
      .countWindow(10)
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
    resultStream.print()
    env.execute("WindowTest")


  }
}
