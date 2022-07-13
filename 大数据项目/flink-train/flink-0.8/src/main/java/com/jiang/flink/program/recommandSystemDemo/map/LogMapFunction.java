package com.jiang.flink.program.recommandSystemDemo.map;

import com.jiang.flink.program.recommandSystemDemo.client.HbaseClient;
import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.util.LogToEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @ClassName LogMapFunction
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 11:35
 * @Version 1.0
 */
@Slf4j
public class LogMapFunction extends RichMapFunction<String , LogEntity> {

    @Override
    public LogEntity map(String value) throws Exception {
        log.info(value);
        LogEntity log = LogToEntity.getLog(value);
        if (null != log){
            String rowKey = log.getUserId() + "_" + log.getProductId()+ "_"+ log.getTime();
            HbaseClient.putData("con",rowKey,"log","userid",String.valueOf(log.getUserId()));
            HbaseClient.putData("con",rowKey,"log","productid",String.valueOf(log.getProductId()));
            HbaseClient.putData("con",rowKey,"log","time",log.getTime().toString());
            HbaseClient.putData("con",rowKey,"log","action",log.getAction());
        }
        return log;
    }
}
