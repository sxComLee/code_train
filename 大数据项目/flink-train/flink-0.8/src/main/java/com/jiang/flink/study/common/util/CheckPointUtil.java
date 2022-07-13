package com.jiang.flink.study.common.util;

import com.jiang.flink.study.common.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;


/**
 * Desc: Checkpoint 工具类
 * Created by zhisheng on 2019-09-06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CheckPointUtil {
    
    /**
     * @Author jiang.li
     * @Description //TODO
     * @Date 08:04 2019-11-16
     * @Param [env, parameterTool]
     * @return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
     **/
    public static StreamExecutionEnvironment setCheckpointConfig(StreamExecutionEnvironment env, ParameterTool parameterTool) throws Exception {
        //只有在checkpoint启用之后才会进行下面的代码执行
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, false)) {
            StateBackend stateBackend = null;
            switch (parameterTool.get(PropertiesConstants.STREAM_CHECKPOINT_TYPE).toLowerCase()) {
                case PropertiesConstants.CHECKPOINT_MEMORY:
                    //设置内存为 5M
                    stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
                    break;
                case PropertiesConstants.CHECKPOINT_FS:
                    stateBackend = new FsStateBackend(new URI(parameterTool.get(PropertiesConstants.STREAM_CHECKPOINT_DIR)),0);
                    break;
                case PropertiesConstants.CHECKPOINT_ROCKETSDB:
                    stateBackend = new RocksDBStateBackend(parameterTool.get(PropertiesConstants.STREAM_CHECKPOINT_DIR));
            }
            //默认checkPoint 时间为 60000ms
            env.enableCheckpointing(parameterTool.getInt(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL,60000));
            //设置内存为状态后端
            env.setStateBackend(stateBackend);

            //高级设置（这些配置也建议写成配置文件中去读取，优先环境变量）
            // 设置 exactly-once 模式
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            // 设置 checkpoint 最小间隔 500 ms
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            // 设置 checkpoint 必须在1分钟内完成，否则会被丢弃
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
            env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
            // 设置 checkpoint 的并发度为 1
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        }

        return env;
    }
}
