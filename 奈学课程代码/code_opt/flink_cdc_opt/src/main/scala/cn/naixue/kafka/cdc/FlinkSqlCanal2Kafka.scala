package cn.naixue.kafka.cdc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

object FlinkSqlCanal2Kafka {
  def main(args: Array[String]): Unit = {
    //构建集群运行的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    //如果要是用cdc直接去处理kafka的数据，需要获取一个StreamTableEnvironment
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()


    //获取StreamTableEnvironment 用于执行cdc，从kafka里面直接获取数据
    val streamTableEnvironment: StreamTableEnvironment = StreamTableEnvironment
      .create(streamEnv, bsSettings)



    //todo:3、通过DDL方式创建
    val createTable=
      """
        |CREATE TABLE t_binlog (
        | id INT NOT NULL,
        | NAME STRING,
        | age INT
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'kafka_cdc',
        | 'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        | 'properties.group.id' = 'testGroup',
        | 'format' = 'canal-json',
        | 'scan.startup.mode' = 'latest-offset'
        |)
      """.stripMargin


    val queryTable="SELECT  * FROM t_binlog"

    streamTableEnvironment.executeSql(createTable)

    //查询表数据
    val table: Table = streamTableEnvironment.sqlQuery(queryTable)

    //提取流数据
    streamTableEnvironment.toRetractStream[Row](table).print

    streamEnv.execute("flinkkakfkacdc")



  }



}
