package com.jiang.flink.program.recommandSystemDemo.map;

import com.jiang.flink.program.recommandSystemDemo.client.HbaseClient;
import com.jiang.flink.program.recommandSystemDemo.client.MysqlClient;
import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.util.LogToEntity;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.ResultSet;

/**
 * @ClassName UserPortraitMapFunction
 * @Description TODO
 *
 * @Author jiang.li
 * @Date 2020-01-10 16:54
 * @Version 1.0
 */
public class UserPortraitMapFunction extends RichMapFunction<String,String> {

    @Override
    public String map(String value) throws Exception {
        LogEntity log = LogToEntity.getLog(value);
        ResultSet rst = MysqlClient.selectById(log.getProductId());
        if (rst != null){
            while (rst.next()){
                String userId = String.valueOf(log.getUserId());

                String country = rst.getString("country");
                HbaseClient.increamColumn("user",userId,"country",country);
                String color = rst.getString("color");
                HbaseClient.increamColumn("user",userId,"color",color);
                String style = rst.getString("style");
                HbaseClient.increamColumn("user",userId,"style",style);
            }

        }
        return null;
    }
}
