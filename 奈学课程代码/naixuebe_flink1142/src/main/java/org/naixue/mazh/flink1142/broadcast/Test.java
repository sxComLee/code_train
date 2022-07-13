package org.naixue.mazh.flink1142.broadcast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class Test {

    public static void main(String[] args) {

        // TODO_MA 马中华 注释：
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataSource<Integer> intDataset = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7);

        // TODO_MA 马中华 注释： base 跟 value 其实原则上来说，在同步的 Task 里面！
        // TODO_MA 马中华 注释： Task 内部会拿到 base 的序列化值   base 这个值会从 driver 传输到 Task
        // TODO_MA 马中华 注释： 这个代码在 客户端执行（Driver JobManager）
        int base = 100;
        // TODO_MA 马中华 注释： 这个base变量的声明和使用不在一个 JVM 的内部，进行序列化
        // TODO_MA 马中华 注释： 万一这个 base 不是一个 int 是一个很大的 set 集合 = 5G

        // TODO_MA 马中华 注释： 算子内部的代码就是在 Task 中执行的
        intDataset.map(new MapFunction<Integer, Integer>(){
            @Override
            public Integer map(Integer value) throws Exception {
                // TODO_MA 马中华 注释： 这个代码是在 Task 里面执行的
                return base + value;
            }
        });

        // TODO_MA 马中华 注释：  我写的这个程序的问题在哪里？
    }
}
