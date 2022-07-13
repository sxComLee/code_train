//package com.jiang.flink.study.scala.core.total
//
//import java.util.function.Predicate
//
//import com.jiang.flink.study.common.util.ExecutionEnvUtil
//import org.apache.flink.api.common.functions.RichFilterFunction
//import org.apache.flink.configuration.Configuration
//
//object getParameter {
//  def main(args: Array[String]): Unit = {
//    val tool = ExecutionEnvUtil.createParameterTool(args)
//
//    val conf = new Configuration()
//    conf.setString("test","Action")
//    val source = ExecutionEnvUtil.getTestSource
//
//    source
//      .filter(new RichFilterFunction[String] {
//      override def filter(t: String): Boolean = {
//        return false;
//      }
//    })
//    //这个方法没办法用
////    .withParameters()
//    ;
//
//
//
//
//  }
//  class FilterGenreWithParameters extends RichFilterFunction[String]{
//
//    var param:String = _;
//    override def open(parameters: Configuration): Unit = {
//      super.open(parameters)
//      param = parameters.getString("test","")
//
//    }
//
//    override def filter(t: String): Boolean = {
//      var params:Array[String] = t.split("\\|");
//
//      return java.util.stream.Stream.of(params).anyMatch(new Predicate[Array[String]] {
//        override def test(t: Array[String]): Boolean = {
//          t.equals(params)
//        }
//      });
//    }
//  }
//
//
//}
