//package com.jiang.flink.study.operate.scala
//
//import com.jiang.flink.study.common.util.ExecutionEnvUtil
//import String._
//
//object OperateTest {
//  def main(args: Array[String]): Unit = {
//    val source = ExecutionEnvUtil.getTestSource
//
//
//    source.map(x => "hello"+x)
//
////    source.flatMap { str => str.split(" ") }
//
//    source.filter( _ != 0)
//
//    source.keyBy(1)
//  }
//}
