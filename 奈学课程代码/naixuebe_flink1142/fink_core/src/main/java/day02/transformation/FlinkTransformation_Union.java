package day02.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 流合并，要求 流的 泛型是一样的。也就是说，只有同种类型的流才能合并
 *  union 和 SQL 中的 union 是一个意思
 *  SQL:  两个查询数据集合并起来
 *  Flink： 两个数据流合并起来
 *  参与合并的两个流的泛型必须一样！
 */
public class FlinkTransformation_Union {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // TODO_MA 马中华 注释： 获取两个 Source DataStream
        DataStreamSource<Long> text1 = env.addSource(new UserDefineSource_NoParallel(100)).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new UserDefineSource_NoParallel(1)).setParallelism(1);

        // TODO_MA 马中华 注释： 把text1和text2组装到一起
        DataStream<Long> text = text1.union(text2);
        // TODO_MA 马中华 注释： 不会去重的  合二为一
        // TODO_MA 马中华 注释： 两条大河 变成一条大河

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据：" + value);
                return value;
            }
        });

        // TODO_MA 马中华 注释： 每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        // TODO_MA 马中华 注释： 打印结果
        sum.print().setParallelism(1);

        // TODO_MA 马中华 注释： 提交
        env.execute("FlinkTransformation_Union");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义一个不带并行度的 Source
     */
    public static class UserDefineSource_NoParallel implements SourceFunction<Long> {

        public UserDefineSource_NoParallel(int start){
            this.number = start;
        }

        private boolean isRunning = true;

        private long number = 1L;

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (isRunning) {
                sourceContext.collect(number++);
                // TODO_MA 马中华 注释： 每隔 1s 钟发送一条数据
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}

