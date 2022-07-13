package com.jiang.flink.program.recommandSystemDemo.map;

import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.util.LogToEntity;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @ClassName GetLogFunction
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 13:41
 * @Version 1.0
 */
public class GetLogFunction extends RichMapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {

        LogEntity log = LogToEntity.getLog(s);
        return log;
    }
}
