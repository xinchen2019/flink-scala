package com.apple

import org.apache.flink.api.common.functions.ReduceFunction

/**
  * @Program: flink-scala
  * @ClassName: MyReducer
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-09 22:31
  * @Version 1.1.0
  **/
class MyReducer extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
  }
}
