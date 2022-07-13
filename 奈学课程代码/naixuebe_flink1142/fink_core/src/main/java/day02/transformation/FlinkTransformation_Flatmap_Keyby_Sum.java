package day02.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Flatmap_Keyby_Sum 算子测试
 *  需求：每隔 1 秒计算最近 2 秒
 */
public class FlinkTransformation_Flatmap_Keyby_Sum {

    public static void main(String[] args) throws Exception {

        String hostname;
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
            hostname = parameterTool.get("hostname");
        } catch (Exception e) {
            System.err.println("no port set,user default port 9988");
            hostname = "bigdata02";
            port = 6789;
        }

        // TODO_MA 马中华 注释： 获取flink运行环境（stream）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // TODO_MA 马中华 注释： 获取数据源
        String delimiter = "\n";
        DataStreamSource<String> textStream = env.socketTextStream(hostname, port, delimiter);

        // TODO_MA 马中华 注释： 执行 transformation 操作
        SingleOutputStreamOperator<WordCount> wordCountStream = textStream.flatMap(new FlatMapFunction<String, WordCount>() {
                    public void flatMap(String line, Collector<WordCount> out) throws Exception {
                        String[] fields = line.split(" ");
                        for (String word : fields) {
                            out.collect(new WordCount(word, 1L));
                        }
                    }
                }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)) //每隔1秒计算最近2秒
                .sum("count");

        wordCountStream.print().setParallelism(1);

        // TODO_MA 马中华 注释： 运行程序
        env.execute("socket word count");
    }

    public static class WordCount {
        public String word;
        public long count;

        public WordCount() {

        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" + "word='" + word + '\'' + ", count=" + count + '}';
        }
    }
}
