package day02.source.user_define;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义的 带并行度 的数据源 测试演示
 */
public class UserDefineSource_WithParallel_Main {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(5);

        // TODO_MA 马中华 注释： Source Operator 被设置为 3 了。
        // TODO_MA 马中华 注释： Source Operator 运行了 3 个 Task
        // TODO_MA 马中华 注释： 每个 Task 里面的逻辑： 都是一样的： 执行 UserDefineSource_WithParallel 的 run()
        // TODO_MA 马中华 注释： 而且，如果这个 Applicatioin 如果在集群中运行，那么大概率这个 3 个 Task 不在一个节点
        // TODO_MA 马中华 注释： 我呢，是在本地运行，最终是通过 3 个线程来模拟实现 3 个Task
        // TODO_MA 马中华 注释： 集群：3 Task， 本地：3 个线程执行 3 个Task
        DataStreamSource<Long> numberDS = executionEnvironment.addSource(new UserDefineSource_WithParallel()).setParallelism(2);

        // TODO_MA 马中华 注释： 先map一下，再filter一下
        // TODO_MA 马中华 注释： map 算子，由于么有设置并行度，所以默认并行度是 全局设置的值：5
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
 *  注释： 自定义带并行度的数据源
 */
class UserDefineSource_WithParallel implements ParallelSourceFunction<Long> {

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