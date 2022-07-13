package com.jiang.flink.study.scala.core.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @ClassName AssignTimestampAndWaterMarkSouce
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-08 13:31
 * @Version 1.0
 */
public class AssignTimestampAndWaterMarkSouce extends RichSourceFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
