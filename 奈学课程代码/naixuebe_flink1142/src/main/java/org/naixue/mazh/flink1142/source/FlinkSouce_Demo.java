package org.naixue.mazh.flink1142.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 */
public class FlinkSouce_Demo {

    public static void main(String[] args) {

        // TODO_MA 马中华 注释： 获取编程入口对象： 环境对象
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 对接数据源，加载数据
        // TODO_MA 马中华 注释： 有四种方式
//        executionEnvironment.readTextFile();          // TODO_MA 马中华 注释： 读文件
//        executionEnvironment.fromCollection();        // TODO_MA 马中华 注释： 读本地集合
//        executionEnvironment.socketTextStream();      // TODO_MA 马中华 注释： 网络端口
//        executionEnvironment.addSource();             // TODO_MA 马中华 注释： 读扩展数据源 或者 自定义数据源

    }
}


