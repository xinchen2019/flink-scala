package com.apple

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}

/**
  * @Program: flink-scala
  * @ClassName: EsOutputTest
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-09-10 15:46
  * @Version 1.1.0
  **/
object EsOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tenv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    val filePath = "data\\sensor\\"

    val source_sql =
      s"""
         |       create table sensor_tb(
         |            id varchar(20),
         |            ts BIGINT,
         |            temperature DOUBLE
         |        ) with (
         |            'connector' = 'filesystem',
         |            'format' = 'csv',
         |            'path' = '${filePath}'
         |        )
      """.stripMargin
    //  'document-type' = 'temperature',  elasticsearch-7：不支持
    val out_sql =
      """
        |CREATE TABLE esOutputTable (
        |  id STRING,
        |  counts BIGINT
        |) WITH (
        |  'connector' = 'elasticsearch-7',
        |  'hosts' = 'http://master:9200',
        |  'index' = 'sensor',
        |  'format' = 'json'
        | )
      """.stripMargin

    val sensor_input = tenv.executeSql(source_sql)

    val resultTable = tenv.sqlQuery("select * from sensor_tb  ")
    val resultTable2 = resultTable.select($"id", $"temperature")
      .filter($"id" === "sensor_1")
      .groupBy($"id")
      .select($"id", $"id".count as "counts") //* tab.groupBy($"key").select($"key", $"value".avg + " The average" as "average")
    resultTable2.printSchema()
    resultTable2.toRetractStream[(String, Long)].print()
    val id2Count_output = tenv.executeSql(out_sql)
    resultTable2.executeInsert("esOutputTable")
    env.execute("BasicTableDemo")
  }
}
