package com.jiang.flink.program;

import com.alibaba.fastjson.JSONObject;
import com.jiang.flink.study.template.FlinkFromKafkaModule;
import com.jiang.flink.study.common.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * @ClassName SessionAnalysis
 * @Description TODO 背景
 *      这几天看到Flink学习群问了一个问题，就是他们想实时监控用户session行为轨迹，如果当前session下用户点击了A事件，如果1小时内用户没有点击B事件，实时流输出C事件
 *      分析
 *          1:flink状态，由于按session聚合，需要使用keyby+process函数
 *          2:通过flink的KeyedProcessFunction内部实现状态管理
 *          3:然后运用KeyedProcessFunction中的定时触发器onTimer，实时定时判断
 *       注意
 *          TimerService 在内部维护两种类型的定时器（处理时间和事件时间定时器）并排队执行。
 *          TimerService 会删除每个键和时间戳重复的定时器，即每个键在每个时间戳
 *          上最多有一个定时器。如果为同一时间戳注册了多个定时器，则只会调用一次 onTimer()方法。
 * @Author jiang.li
 * @Date 2019-11-14 15:31
 * @Version 1.0
 */
public class SessionAnalysis extends FlinkFromKafkaModule {
    private String sessionId = "";
    private String event_id = "";
    private long timestamp = 0L;

    public SessionAnalysis(String sessionId, String event_id, long timestamp) {
        this.sessionId = sessionId;
        this.event_id = event_id;
        this.timestamp = timestamp;
    }

    public static void main(String[] args) {
        try {
            String jobName = "SessionAnalysis";
            //获取kafka配置信息
            String groupId = "SessionAnalysis";
            //设置kakfa对应的broker地址
            String bootStrapServer = " ";

            String topics = " ";

            KafkaUtil kafkaUtil = new KafkaUtil(groupId, bootStrapServer,topics);

            CountFlow overviewFlink = new CountFlow();

            overviewFlink.commonDeal(jobName, kafkaUtil,null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //数据格式如下 jsonObject.put("session_id",session_id);
    //          put("event_id",event_id)
    @Override
    public SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource, StreamExecutionEnvironment env) {
        SingleOutputStreamOperator text = kafkaSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                return new Tuple2<>(jsonObject.getString("session_id"), jsonObject.getString("event_id"));
            }
        }).assignTimestampsAndWatermarks(new MyTimeTimestampsAndWatermarks());


        SingleOutputStreamOperator process = text.keyBy(0).process(new SessionIdTimeoutFunciton());
        return process;
    }

    /**
     * @Author jiang.li
     * @Description //TODO  由于是按key聚合，创建每个key的状态 key=session_id 实现KeyedProcessFunction内的onTime方法
     * @Date 15:53 2019-11-14
     * @Param
     * @return
     **/
    class SessionIdTimeoutFunciton extends KeyedProcessFunction<Tuple,Tuple2<String,String>,Tuple2<String,String>>{

        private ValueState<SessionAnalysis> state = null;
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //按天做统计，所以失效时间为一天
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    //创建或者更改的时候进行更改
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    //状态可见性配置是否可以返回过期的用户值
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            //值状态可以保存的值的类型   状态名 mystate， 状态类型为提示类信息

            ValueStateDescriptor<SessionAnalysis> stateDescriptor = new ValueStateDescriptor<>("myState", SessionAnalysis.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            state = getRuntimeContext().getState(stateDescriptor);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            //如果当前key，5秒之后，没有触发B事件
            //并且事件一定到了触发的事件点，就输出C事件
            System.out.println("onTimer触发时间：状态记录的时间_触发时间"+sdf.format(new Date(state.value().timestamp)) + "_" +sdf.format(new Date(timestamp)));
            if(state.value().event_id !="B" && state.value().timestamp +5000 == timestamp){
                out.collect(new Tuple2<>("SessionID为："+ state.value().sessionId,"由于5s内没有看到B触发C时间"));
            }
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if(state.value() == null){
                //获取当前时间
                Long timestamp = ctx.timestamp();
                //输出当前实时流事件，这次没有考虑事件先后顺序
                //如果要对事件先后顺序加一下限制，state需要重新设计
                //这次就简单实现一下原理，后边我再写一个针对顺序的代码
                out.collect(value);

                if(value.f0 == "A" ){
                    //如果状态是A,设置下次回调的时间。5秒之后回调
                    ctx.timerService().registerEventTimeTimer(timestamp+5000);
                    state.update(new SessionAnalysis(value.f0,value.f1,timestamp));
                }
                //如果发现当前sessionid下有B行为，就更新B
                System.out.println("当前时间："+sdf.format(new Date(timestamp)));
                if(value.f1 == "B"){
                    state.update(new SessionAnalysis(value.f0,value.f1,timestamp));
                }

            }
        }
    }

    /**
     * @Author jiang.li
     * @Description //TODO 指定timeStamp和waterMark
     * @Date 15:46 2019-11-14
     * @Param
     * @return
     **/
    class MyTimeTimestampsAndWatermarks implements AssignerWithPunctuatedWatermarks{

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Object lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp -1000);
        }
        //生成时间戳
        @Override
        public long extractTimestamp(Object element, long previousElementTimestamp) {
            return System.currentTimeMillis();
        }
    }
}
