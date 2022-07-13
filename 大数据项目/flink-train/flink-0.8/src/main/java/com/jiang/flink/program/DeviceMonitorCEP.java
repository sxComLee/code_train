package com.jiang.flink.program;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jiang.flink.program.bean.RegionInfo;
import com.jiang.flink.study.template.FlinkFromKafkaModule;
import com.jiang.flink.study.sink.HbaseSink;
import com.jiang.flink.study.common.util.KafkaUtil;
import com.jiang.flink.study.source.MysqlSource;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * @ClassName AlertExcept
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-15 10:25
 * @Version 1.0
 */
public class DeviceMonitorCEP extends FlinkFromKafkaModule {
    public static void main(String[] args) {
        try {
            String jobName = "com.fengjr.flink.stream.operate.community.AlertExcept execute";
            //获取kafka配置信息
            String groupId = " ";
            //设置kakfa对应的broker地址
            String bootStrapServer = " ";

            String topics = " ";

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

    @Override
    public SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource, StreamExecutionEnvironment env) {
        //将信息广播
        //broadcast能全部分发到partition。
        MapStateDescriptor<String, RegionInfo> descriptor =
                new MapStateDescriptor<>("region_rule", BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<RegionInfo>() {}));

        BroadcastStream<RegionInfo> regionStream = env.addSource(new MysqlSource()).broadcast(descriptor);

        kafkaSource.connect(regionStream)
                .process(new BroadcastProcessFunction<String, RegionInfo, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        JSONObject json = JSONObject.parseObject(value);
                        JSONArray arrayJson = json.getJSONArray("risk");
                        for (int i=0;i<=arrayJson.size() ; i++ ) {
                            JSONObject obj = arrayJson.getJSONObject(i);
                            Iterator<Map.Entry<String, RegionInfo>> state = ctx.getBroadcastState(descriptor).immutableEntries().iterator();
                            while(state.hasNext()){
                                Map.Entry<String, RegionInfo> map = state.next();
                                obj.put("inner_code",map.getValue().getInnerCode());
                                out.collect(obj.toJSONString());
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(RegionInfo value, Context ctx, Collector<String> out) throws Exception {
                        String regionInfo = value.getRegionInfo();
                        BroadcastState<String, RegionInfo> state = ctx.getBroadcastState(descriptor);
                        state.put(regionInfo+"_"+value.getInnerCode(),value);
                    }
                }).setParallelism(1).print();


        return null;
    }
}
