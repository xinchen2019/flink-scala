package com.apple

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Program: flink-scala
  * @ClassName: StateTest
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-09 21:35
  * @Version 1.1.0
  **/
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.socketTextStream("master", 9999)
    inputStream.print()
    env.execute("StateTest")
  }
}

class MyRichMapper1 extends RichMapFunction[SensorReading, String] {

  lazy val listState = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  lazy val mapState = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState = getRuntimeContext.getReducingState(
    new ReducingStateDescriptor[SensorReading]("reducstate", new MyReducer, classOf[SensorReading]))
  var valueState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    //状态的读写
    val myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)
    listState.get()

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    reduceState.get()
    reduceState.add(value)


    value.id
  }
}

