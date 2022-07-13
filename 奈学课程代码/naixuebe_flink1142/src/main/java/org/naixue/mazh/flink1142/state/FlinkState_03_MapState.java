package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：每 3 个相同 key 就输出他们的 平均值
 *  使用 MapState 来进行分组聚合计算
 */
public class FlinkState_03_MapState {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释：
        DataStreamSource<Tuple2<Long, Long>> sourceDS = environment.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(1L, 5L), Tuple2.of(2L, 3L), Tuple2.of(2L, 5L)
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 使用三种 State 来计算
         *  1、ValueState 只会存储一个单一的值，注意，一个 Key 一个 ValueState
         *  2、ListState 存储了数据列表，注意，一个 Key 一个 ListState
         *  3、MapState 存储数据集合，注意，一个 Key 一个 MapState
         */
        SingleOutputStreamOperator<Tuple2<Long, Double>> resultDS = sourceDS.keyBy(0)
                .flatMap(new CountAverageWithMapState());

        // TODO_MA 马中华 注释：
        resultDS.print();

        // TODO_MA 马中华 注释：
        environment.execute("FlinkState_MapState");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 MpaState 处理计算
     */
    static class CountAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        // TODO_MA 马中华 注释：
        private MapState<String, Long> mapState;
        // TODO_MA 马中华 注释： 一个 key 一个 mapState
        // TODO_MA 马中华 注释： 用 MapState，需要去存储 输入数据中的 第二个元素。
        // TODO_MA 马中华 注释： 第一个元素是 key 其实没有用的

        @Override
        public void open(Configuration parameters) throws Exception {
            // TODO_MA 马中华 注释：
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("map_state", String.class, Long.class);
            // TODO_MA 马中华 注释：
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> record, Collector<Tuple2<Long, Double>> collector) throws Exception {
            // 记录状态数据
            mapState.put(UUID.randomUUID().toString(), record.f1);
            // 执行判断
            Iterable<Long> values = mapState.values();
            ArrayList<Long> dataList = Lists.newArrayList(values);

            int count = 3;
            if (dataList.size() == count) {
                long total = 0;
                for (Long data : dataList) {
                    total += data;
                }
                double avg = (double) total / count;
                collector.collect(Tuple2.of(record.f0, avg));
                mapState.clear();
            }
        }
    }
}

// TODO_MA 马中华 注释： 区别就在于， 你怎么去用 Value  List  Map 这种数据结构来存储状态
// TODO_MA 马中华 注释： 一个 key 一个 ListState 或者 ValueState 或者 MapState

// TODO_MA 马中华 注释： Map<Key, MapState<String, Long>> stateContainer 就是这个 Task 的所有状态数据
// TODO_MA 马中华 注释： 如果突然某个时刻这个 Task 死掉了， stateContainer 就丢失了

// TODO_MA 马中华 注释： 必然要 持久化状态  Checkpoint