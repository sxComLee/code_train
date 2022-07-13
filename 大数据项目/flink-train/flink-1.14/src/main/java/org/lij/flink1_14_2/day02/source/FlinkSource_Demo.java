package org.lij.flink1_14_2.day02.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-26 22:50
 */
public class FlinkSource_Demo {
    public static void main(String[] args) {
        // 获取环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 对接数据源，加载数据 ===> 四种方式
        env.readTextFile("");    // 读文件
        env.fromCollection(null);  // 读取本地集合
        env.socketTextStream("",0); // 网络端口
        env.addSource(null);  // 扩展数据源 或者自定义数据源
    }
}
