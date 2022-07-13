package com.jiang.flink.study.common.util;


import com.jiang.flink.study.common.constant.PropertiesConstants;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ExecutionEnvUtil {

    public static ParameterTool createParameterTool(){
        return createParameterTool(null);

    }

    public static ParameterTool createParameterTool(String[] args)  {
        //从系统参数中获取
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();

        try{
            parameterTool = parameterTool
                    //从配置文件中获取
                    .mergeWith(parameterTool.fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME)));
            if (null != args) {
                //从传入的数组中获取
                parameterTool = parameterTool.mergeWith(ParameterTool.fromArgs(args));
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return parameterTool;

    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();
    /**
     * @return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
     * @Author jiang.li
     * @Description //TODO 获取StreamExecutionEnvironment对象
     * @Date 10:29 2019-11-16
     * @Param [parameterTool]
     **/
    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        ExecutionConfig conf = env.getConfig();
        //禁止运行过程中的日志输出
        // conf.disableSysoutLogging();

        //设置重启策略
        conf.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));
        //设置启用checkPoint
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            //设置checkpoint
            CheckPointUtil.setCheckpointConfig(env, parameterTool);
        }
        //设置全局的参数
        conf.setGlobalJobParameters(parameterTool);
        //设置一次性语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }

    public static DataStream<String> getTestSource(){

        String hostName = "localhost";
        int port = Integer.parseInt("8001");

        //set up the streaming execution evironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return env.socketTextStream(hostName, port);
    }

}