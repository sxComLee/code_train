package com.jiang.flink.program.recommandSystemDemo.map;

import com.jiang.flink.program.recommandSystemDemo.client.HbaseClient;
import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.util.LogToEntity;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @ClassName UserHistoryMapFunction
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 13:26
 * @Version 1.0
 */
public class UserHistoryMapFunction extends RichMapFunction<String,String> {


    @Override
    public String map(String value) throws Exception {
        LogEntity log = LogToEntity.getLog(value);
        if (null != log){
            HbaseClient.increamColumn("u_history",String.valueOf(log.getUserId()),"p",String.valueOf(log.getProductId()));
            HbaseClient.increamColumn("p_history",String.valueOf(log.getProductId()),"p",String.valueOf(log.getUserId()));
        }
        return "";
    }
}
