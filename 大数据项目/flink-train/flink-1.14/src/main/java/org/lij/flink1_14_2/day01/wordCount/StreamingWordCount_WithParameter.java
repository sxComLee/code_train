package org.lij.flink1_14_2.day01.wordCount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-26 22:26
 */
public class StreamingWordCount_WithParameter {
    public static void main(String[] args) throws Exception{
        // 通过参数工具获取对象
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        // 获取环境执行对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Student> wordOneDS = source.flatMap((line, collector) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(new Student(word, 1));
            }
        });

        SingleOutputStreamOperator<Student> resultDS = wordOneDS.keyBy("name").sum("count").setParallelism(3);

        resultDS.print().setParallelism(5);

        env.execute("Flink Streaming WordCount");
    }

    public static class Student{
        String name;
        int count;

        public Student() {
        }

        public Student(String name,int count){
            this.name = name;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
