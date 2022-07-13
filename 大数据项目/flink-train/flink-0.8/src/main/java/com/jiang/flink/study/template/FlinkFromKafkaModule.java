package com.jiang.flink.study.template;


import com.jiang.flink.study.common.util.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName EnvUtil
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-07 09:41
 * @Version 1.0
 */
public abstract class FlinkFromKafkaModule {

    /**
     * @Author jiang.li
     * @Description //TODO 通用的处理方式
     * @Date 16:43 2019-11-12
     * @Param [jobName, kafkaUtil]
     * @return void
     **/
    public void commonDeal(String jobName, KafkaUtil kafkaUtil, SinkFunction sinkFunction) throws Exception{
        //创建环境
        StreamExecutionEnvironment env = getStreamEnv();
        //获取kafkaSouce
        DataStreamSource<String> kafkaSource = getKafkaSource(kafkaUtil,env);
        //对数据源进行处理
        SingleOutputStreamOperator dealedSource = dealStream(kafkaSource,env);

        //将数据进行输出
        dealedSource.addSink(sinkFunction);
//        dealedSource.print();

        env.execute(jobName);
    }
    
    /**
     * @Author jiang.li
     * @Description //TODO 继承的类根据自己的实际进行数据处理
     * @Date 16:43 2019-11-12
     * @Param []
     * @return void
     **/
    public abstract SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource,StreamExecutionEnvironment env);

    /**
     * @Author jiang.li
     * @Description //TODO 获取flink的执行环境
     * @Date 16:42 2019-11-12
     * @Param []
     * @return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
     **/
    public StreamExecutionEnvironment getStreamEnv() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //================  关于时间  =====================
        //设置使用eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //================  关于checkpoint   =====================
        // 设置checkpoint1s一次，且保证精确一次，即barrier要对齐
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);

        //获取checkpoint相关设置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        //设置两个checkpoint之间最少的等待时间
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        //设置checkpoint的最长时间为1min，超时即为checkpoint失败
        checkpointConfig.setCheckpointTimeout(1000 * 60);

        //checkpoint失败的时候不能报错
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        //如果当前进程正在checkpoint，系统不会触发另一个checkpoint
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        //确保数据严格一次
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //下次 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //================  关于状态后端   =====================
        //状态后端通过 flink-conf.yaml 配置文件指定或者代码
        //创建对应的状态后端对象
//        String rockURL = ConfManager.getProperty(Constant.ROCKDB_BACKEND_STATE_URL);
//        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(rockURL);

//        //设置对应的状态后端
//        env.setStateBackend((StateBackend) rocksDBStateBackend);

        return env;
    }


    /**
     * @Author jiang.li
     * @Description //TODO 获取通用的kafkaSource
     * @Date 16:24 2019-11-12
     * @Param [groupId, bootStrapServer, clientId]
     * @return org.apache.flink.streaming.api.datastream.DataStreamSource<java.lang.String>
     **/
    public DataStreamSource<String> getKafkaSource(KafkaUtil kafkaUtil,StreamExecutionEnvironment streamEnv ) throws IOException{
        //获取kafka属性
        Properties kafkaProperties = kafkaUtil.getKafkaProperties();
        //获取对应的kafka主题
        String kafkaTopics = kafkaUtil.getTopics();
        List<String> topicList = Arrays.asList(kafkaTopics.split(","));
        //创建flink消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicList, new SimpleStringSchema(), kafkaProperties);

        DataStreamSource<String> kafkaSource = streamEnv.addSource(consumer);
        return kafkaSource;
    }


}
