package com.jiang.flink.program;

import com.alibaba.fastjson.JSONObject;
import com.jiang.flink.study.template.FlinkFromKafkaModule;
import com.jiang.flink.study.sink.HbaseSink;
import com.jiang.flink.study.common.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

/**
 * @ClassName CountFlow
 * @Description TODO
 * 项目背景：众所周知，各大电商app在首页都会设置"金刚区"，而金刚区可以算是流量中的“黄金位置”，乃至“钻石位置”。
 *      下图是某电商app的首页截图，其金刚由10大导航icon组成，其集团内业务板块众多，金刚区icon数量有限，因此icon的取舍需要靠数据评估。
 *      今天我就拿这个来做一个模拟实战——用一个指标来评估这个icon的点击情况，从而决定这个icon的去留。
 * 项目理解：
 *      用户来到电商app，可以理解为一次请求sid，也可以理解为session，用户会可能会重复点击多个icon，业务想实时查看每个icon下有多个去重复的请求sid。
 * @Author jiang.li
 * @Date 2019-11-14 14:23
 * @Version 1.0
 */
public class CountFlow extends FlinkFromKafkaModule {

    public static void main(String[] args) {
        try {
            String jobName = "com.fengjr.flink.stream.operate.community.Overview execute";
            //获取kafka配置信息
            String groupId = "";
            //设置kakfa对应的broker地址
            String bootStrapServer = "";

            String topics = "";

            KafkaUtil kafkaUtil = new KafkaUtil(groupId, bootStrapServer,topics);

            CountFlow overviewFlink = new CountFlow();
            String hbaseName = "";
            String cloumnFamily = "";

            HbaseSink hbaseSink = new HbaseSink(hbaseName,cloumnFamily);

            overviewFlink.commonDeal(jobName, kafkaUtil,hbaseSink);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //数据格式如下 jsonObject.put("sid",sid);
                //          put("icon",messageList.get(random_icon))
                //          put("event_time",System.currentTimeMillis())
                //          put("event_day","当天日期")
    @Override
    public SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource, StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> process = kafkaSource.map(new MapFunction<String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                return new Tuple4<>(jsonObject.getString("sid"), jsonObject.getString("icon")
                        , jsonObject.getString("event_time"), jsonObject.getString("event_day"));
            }
        }).keyBy(0, 1)
                .process(new KeyedProcessFunction<Tuple, Tuple4<String, String, String, String>, Tuple4<String, String, String, Long>>() {
                    private ValueState<Tuple2<String, String>> state;

                    //为ValueState赋值
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //按天做统计，所以失效时间为一天
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                                //创建或者更改的时候进行更改
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //状态可见性配置是否可以返回过期的用户值
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        //值状态可以保存的值的类型   状态名 mystate， 状态类型为提示类信息
                        ValueStateDescriptor<Tuple2<String, String>> stateDescriptor =
                                new ValueStateDescriptor<Tuple2<String, String>>("myState", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                                }));
                        //状态值开启ttl
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        //创建状态
                        state = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(Tuple4<String, String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
                        if (state.value() == null) {
                            out.collect(new Tuple4<>(value.f1, value.f2, value.f3, 1L));
                            state.update(new Tuple2<>(value.f0, value.f1));
                        }
                    }
                });

        SingleOutputStreamOperator<String> map = process
                //根据第1，2，3个属性创建一个keyedStream
                .keyBy(0, 1, 2)
                //10s一个无重复的window
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                //按照第4个属性进行统计
                .sum(3).map(new MapFunction<Tuple4<String, String, String, Long>, String>() {
                    @Override
                    public String map(Tuple4<String, String, String, Long> o) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("icon", o.f0);
                        jsonObject.put("dt", o.f1);
                        jsonObject.put("hour", o.f2);
                        jsonObject.put("sid_num", o.f3);
                        return jsonObject.toJSONString();
                    }
                });
        map.print();
        return map;
    }
}
