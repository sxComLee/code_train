//package com.jiang.flink.study.scala.core
//
//import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
//import java.{lang, util}
//import java.util.{Properties, Timer, TimerTask}
//
//import com.jiang.flink.study.common.util.ExecutionEnvUtil
//import lombok.extern.slf4j.Slf4j
//import org.apache.flink.api.common.functions.RichMapFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.kafka.common.serialization.StringSerializer
//
///**
//  * 定时查询mysql，获取维度数据
//  */
//@Slf4j
//object OnTimeLoadMysqlSource {
//  def main(args: Array[String]): Unit = {
//    val tool = ExecutionEnvUtil.createParameterTool(args)
//    val env = ExecutionEnvUtil.prepare(tool)
//
//    val input = env.addSource(new TwoStringSource)
//
//    val stream = input.map(new RichMapFunction[String, String] {
//      val jdbcUrl = "jdbc:mysql://venn:3306?useSSL=false&allowPublicKeyRetrieval=true"
//      val username = "root"
//      val password = "123456"
//      val driverName = "com.mysql.jdbc.Driver"
//      var conn: Connection = null
//      var ps: PreparedStatement = null
//      val map = new util.HashMap[String, String]()
//
//      override def open(parameters: Configuration): Unit = {
//        super.open(parameters)
//        val timer = new Timer(true)
//        timer.schedule(new TimerTask {
//          override def run(): Unit = {
//
//          }
//        }, 1000)
//      }
//
//      override def map(value: String): String = {
//        value + "_" + map.get(value.split(",")(0))
//      }
//
//      def query() = {
//        try {
//          Class.forName("com.mysql.jdbc.Driver")
//          conn = DriverManager.getConnection(jdbcUrl)
//          ps = conn.prepareStatement("select id,name from venn.timer")
//          val rs = ps.executeQuery()
//
//          while (!rs.isClosed && rs.next()) {
//            val id = rs.getString(1)
//            val name = rs.getString(2)
//            map.put(id, name)
//          }
//
//        } catch {
//          case e@(_: ClassNotFoundException | _: SQLException) =>
//            e.printStackTrace()
//
//        } finally {
//          ps.close()
//          conn.close()
//        }
//
//      }
//
//    })
//
//
//    var sink = new FlinkKafkaProducer[String]("timer"
//      ,new KafkaSerializationSchema[String] {
//        override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]]={
//            return null;
//      }}
//      ,new Properties()
//      ,FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//     )
//
//    stream.addSink(sink)
//
//
//
//  }
//
//  class TwoStringSource extends RichSourceFunction[String] {
//    override def run(ctx: SourceFunction.SourceContext[String]): Unit = ???
//
//    override def cancel(): Unit = ???
//  }
//
//}
