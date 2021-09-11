package com.apple

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Program: flink-scala
  * @ClassName: StreamWordCount
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-08 18:09
  * @Version 1.1.0
  **/
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    val paramTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")
    val inputDataStream = env.socketTextStream(host, port)

    import org.apache.flink.api.scala._

    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    resultDataStream.print().setParallelism(1)
    env.execute("stream word count")
  }
}
