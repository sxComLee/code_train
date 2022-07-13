package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：每 3 个相同 key 就输出他们的 平均值
 */
public class FlinkState_02_ListState {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释：
        DataStreamSource<Tuple2<Long, Long>> sourceDS = environment.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L),
                Tuple2.of(2L, 3L),
                Tuple2.of(2L, 5L)
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 使用三种 State 来计算
         *  1、ValueState 只会存储一个单一的值，注意，一个 Key 一个 ValueState
         *  2、ListState 存储了数据列表，注意，一个 Key 一个 ListState
         *  3、MapState 存储数据集合，注意，一个 Key 一个 MapState
         */
        SingleOutputStreamOperator<Tuple2<Long, Double>> resultDS = sourceDS.keyBy(0).
                // TODO_MA 马中华 注释： 和第一个例子中，唯一不一样的地方：
                // TODO_MA 马中华 注释： 第一个例子： ValueState
                // TODO_MA 马中华 注释： ListState
                flatMap(new CountAverageWithListState());

        // TODO_MA 马中华 注释：
        resultDS.print();

        // TODO_MA 马中华 注释：
        environment.execute("FlinkState_ListState");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    static class CountAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        // TODO_MA 马中华 注释： 存储多个值，通过 List 来组织，然后每个元素的类型： Tuple2<Long, Long>
        private ListState<Tuple2<Long, Long>> listState;
        // TODO_MA 马中华 注释： 在这个例子当中，这个 ListState 就是输入数据的类型
        // TODO_MA 马中华 注释： 一个 key 一个 ListState

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Tuple2<Long, Long>> listStateDesc = new ListStateDescriptor<>("avg_liststate",
                    Types.TUPLE(Types.LONG, Types.LONG)
            );
            listState = getRuntimeContext().getListState(listStateDesc);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> record, Collector<Tuple2<Long, Double>> collector) throws Exception {
            // 初始化状态
            Iterable<Tuple2<Long, Long>> dataIterator = listState.get();
            if (dataIterator == null) {
                listState.addAll(Collections.EMPTY_LIST);
            }

            // 将数据加入到状态
            // TODO_MA 马中华 注释： 每次接收到一条数据，就加入到 ListState
            // TODO_MA 马中华 注释： 然后再去判断，这个 ListState 存储的元素的个数，是否满足计算要求了，是，则执行计算，输出结果
            listState.add(record);

            // 获取状态数据集
            ArrayList<Tuple2<Long, Long>> dataList = Lists.newArrayList(listState.get());
            // 判断是否满足条件
            int count = 3;
            if (dataList.size() == count) {
                // TODO_MA 马中华 注释： 求得这个 key 的 value 总和
                long total = 0;
                for (Tuple2<Long, Long> data : dataList) {
                    total += data.f1;
                }
                // TODO_MA 马中华 注释： 计算平均值
                double avg = (double) total / count;
                // TODO_MA 马中华 注释： 做输出
                collector.collect(Tuple2.of(record.f0, avg));
                // TODO_MA 马中华 注释： 状态清空
                listState.clear();
            }
        }
    }
}
