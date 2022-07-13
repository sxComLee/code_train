package org.naixue.mazh.flink1142.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 */
public class DataSetTransform_Distinct {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        ArrayList<String> data = new ArrayList<>();
        data.add("you jump");
        data.add("i jump");
        DataSource<String> text = env.fromCollection(data);

        // TODO_MA 马中华 注释：
        FlatMapOperator<String, String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.toLowerCase().split("\\W+");
                for (String word : split) {
                    System.out.println("单词：" + word);
                    out.collect(word);
                }
            }
        });

        // TODO_MA 马中华 注释： 对数据进行整体去重
        flatMapData.distinct().print();
    }
}
