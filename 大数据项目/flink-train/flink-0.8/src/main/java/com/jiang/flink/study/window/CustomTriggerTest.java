package com.jiang.flink.study.window;

import com.jiang.flink.study.common.model.WordEvent;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.window.function.CustomSource;
import com.jiang.flink.study.window.function.CustomTrigger;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @ClassName CustomTriggerTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-29 16:51
 * @Version 1.0
 */
public class CustomTriggerTest {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        //不这是时间默认是ProcessTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<WordEvent> data = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WordEvent>() {
                    private long currentTimestamp = Long.MIN_VALUE;

                    private final long maxTimeLag = 5000;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MAX_VALUE : currentTimestamp - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(WordEvent element, long previousElementTimestamp) {
                        //数据清理，有的数据可能对应的时间字段为空
                        //但是这个问题应该在map的时候就行处理，赋予初值
                        if (element.getTimestamp() > currentTimestamp) {
                            this.currentTimestamp = element.getTimestamp();
                        }
                        return currentTimestamp;
                    }
                });
        data.keyBy(WordEvent::getWord)
                .timeWindow(Time.seconds(10))
                .trigger(CustomTrigger.create())
                .sum("count")
                .print();

        env.execute("custom Trigger window demo");
    }
}
