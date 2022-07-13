package day01.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Flink-1.14 版本的 流式 wordcount 入门案例
 *  测试的时候，运行：nc -lk 6789
 */
public class StreamingWordCount_Object2 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 加载数据源获取数据抽象对象
        // TODO_MA 马中华 注释： 其实 DataStreamSource 就是一个 DataStream
        DataStreamSource<String> sourceDataStream = executionEnvironment.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释： 执行逻辑处理
        SingleOutputStreamOperator<WordAndCount> resultDS = sourceDataStream.flatMap(new SplitFunction()).keyBy("word").sum("count");

        // TODO_MA 马中华 注释： 结果输出
        resultDS.print().setParallelism(2);

        // TODO_MA 马中华 注释： 提交执行
        executionEnvironment.execute("Flink Streaming WordCount");
    }

    public static class SplitFunction implements FlatMapFunction<String, WordAndCount>{

        @Override
        public void flatMap(String line, Collector<WordAndCount> collector) throws Exception {
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(new WordAndCount(word, 1));
            }
        }
    }

    public static class WordAndCount {
        private String word;
        private int count;

        public WordAndCount() {
        }

        public WordAndCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" + "word='" + word + '\'' + ", count=" + count + '}';
        }
    }
}
