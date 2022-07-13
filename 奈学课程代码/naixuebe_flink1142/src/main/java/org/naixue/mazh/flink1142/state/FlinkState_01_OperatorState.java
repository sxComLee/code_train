package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求： 每 N 条数据打印输出一次
 *  自定义的 Print 操作
 *  1、Flink 提供的 print:  一条一条的输出， 没有状态数据
 *  2、我的需求： 每隔 3 条输出一次，  接收到 第一条，第二条不输出，等接收到 第三条一起输出
 *  -
 *  因为我么没有还是用到 keyBy 所以就不是 keyedState  是  OperatorState
 *  1、keyedState ： 一个 key 一个 state(List， Value, Map,.....)
 *  2、OperatorState ：  一个 Task 一个 state
 */
public class FlinkState_01_OperatorState {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.fromElements(
                Tuple2.of("Spark", 3),
                Tuple2.of("Flink", 5),
                Tuple2.of("Hadoop", 7),
                Tuple2.of("Hive", 6),
                Tuple2.of("HBase", 9),
                Tuple2.of("Spark", 4)
        );

        // TODO_MA 马中华 注释： N = 3
        dataStreamSource.addSink(new UDPrintSink(2)).setParallelism(1);
        // TODO_MA 马中华 注释： 在 sink Operator 的每一个 Task 里面去维护一个 ListState 去做

        // TODO_MA 马中华 注释： 提交
        env.execute("FlinkState_OperatorState");
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义一个状态管理器
 *  1、Task 有自己的状态
 *  2、Key 有自己的状态。
 *  最怕的就是 Task 突然死掉，状态数据丢失
 *  既然要保证状态数据的安全，所以加入 state 持久化机制！
 *  到底设么是 checkpoint 其实就是吧每个 Task 自己身上的 state 给持久化起来
 *  -
 *  CheckpointedFunction
 *  1、持久化的方法： 把 state 存起来
 *  2、恢复的方法： 从 state 的存储地 再恢复回来
 */
class UDPrintSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    // TODO_MA 马中华 注释： 输出的 N 定义
    private int recordNumber;

    // TODO_MA 马中华 注释： 数据容器
    private List<Tuple2<String, Integer>> bufferElements;

    // TODO_MA 马中华 注释： ListState 状态
    private ListState<Tuple2<String, Integer>> listState;

    // TODO_MA 马中华 注释： 构造方法
    UDPrintSink(int recordNumber) {
        this.recordNumber = recordNumber;
        this.bufferElements = new ArrayList<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // TODO_MA 马中华 注释： 针对数据集合，拍摄 state 的快照
        listState.clear();
        for (Tuple2<String, Integer> ele : bufferElements) {
            listState.add(ele);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        // TODO_MA 马中华 注释： 初始化 ListState
        ListStateDescriptor<Tuple2<String, Integer>> listStateDescriptor = new ListStateDescriptor<>("UDPrintSink",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })
        );
        // TODO_MA 马中华 注释： 初始化 ListState
        this.listState = context.getOperatorStateStore().getListState(listStateDescriptor);

        // TODO_MA 马中华 注释： 状态恢复
        if (context.isRestored()) {
            for (Tuple2<String, Integer> ele : listState.get()) {
                bufferElements.add(ele);
            }
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        // TODO_MA 马中华 注释： 数据加入到 bufferElements 数据集合中
        bufferElements.add(value);

        if (bufferElements.size() == recordNumber) {
            System.out.println("自定义格式：" + bufferElements);
            bufferElements.clear();
        }
    }
}
