package com.apple.udf


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * @Program: flink-scala
  * @ClassName: ProcessWinFunctionOnWindowDemo
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-14 10:21
  * @Version 1.1.0
  *          参考链接：https://www.cnblogs.com/linjiqin/p/12591729.html
  **/


object ProcessWinFunctionOnWindowDemo {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val englishData = Array(
      "class1,张三,100",
      "class1,李四,78",
      "class1,王五,99",
      "class2,赵六,81",
      "class2,小七,59",
      "class2,小八,97"
    )

    val englishDataStream = env.fromCollection(englishData)
      .map(line => {
        val arr = line.split(",")
        (arr(0), arr(1), arr(2).toLong)
      })

    val avgScore = englishDataStream
      .keyBy(_._1)
      .countWindow(2)
      .process(new MyProcessWindowFunction)
    avgScore.print()
    env.execute("ProcessWinFunctionOnWindowDemo")
  }
}


class MyProcessWindowFunction extends ProcessWindowFunction[(String, String, Long), (String, Double), String, GlobalWindow] {

  override def process(key: String, context: Context, elements: Iterable[(String, String, Long)], out: Collector[(String, Double)]): Unit = {
    println("process================")
    var sum: Long = 0L
    var count: Long = 0L
    var clazz: String = ""
    for (in: (String, String, Long) <- elements) {
      clazz = in._1
      sum += in._3
      count += 1
    }
    out.collect((clazz, (sum / count)))
  }
}

