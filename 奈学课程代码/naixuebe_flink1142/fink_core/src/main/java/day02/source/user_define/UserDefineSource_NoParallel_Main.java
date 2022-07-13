package day02.source.user_define;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义的 不带并行度 的数据源 测试演示
 *  Source Operator 的 Task 只有一个！
 *  1、单并行度： MySource implements SourceFunction<T>
 *  2、多并行度： MySource implements ParallelSourceFunction<T>
 */
public class UserDefineSource_NoParallel_Main {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 这个数据源：有节奏的每隔 1s 输出一个顺序递增的自然数
        DataStreamSource<Long> numberDS = executionEnvironment.addSource(new UserDefineSource_NoParallel())
                .setParallelism(1);

        // TODO_MA 马中华 注释： 先map一下，再filter一下
        SingleOutputStreamOperator<Long> resultDS = numberDS.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到了数据：" + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        }).setParallelism(2);

        // TODO_MA 马中华 注释：
        resultDS.print().setParallelism(2);

        // TODO_MA 马中华 注释：
        executionEnvironment.execute("UserDefineSource_NoParallel_Main");
    }
}


/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义一个不带并行度的 Source
 *  实现了这个 SourceFunction 的 Source 就是但并行度，就算设置了 N > 1 的并行度也没用！
 */
class UserDefineSource_NoParallel implements SourceFunction<Long> {

    private boolean isRunning = true;

    private long number = 1L;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        // TODO_MA 马中华 注释： 一般来说，肯定是从哪个地方读取数据
        // TODO_MA 马中华 注释： 模拟实现：每隔 1s 输出一条数据（顺序递增的数字）
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