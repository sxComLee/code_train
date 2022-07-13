package com.jiang.flink.program.recommandSystemDemo.map;


import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author XINZE
 */
public class TopProductMapFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);
        return log;
    }
}
