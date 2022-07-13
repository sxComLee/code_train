package com.jiang.flink.program.recommandSystemDemo.map;

import com.jiang.flink.program.recommandSystemDemo.client.HbaseClient;
import com.jiang.flink.program.recommandSystemDemo.client.MysqlClient;
import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.util.AgeUtil;
import com.jiang.flink.program.recommandSystemDemo.util.LogToEntity;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.ResultSet;

/**
 * @ClassName ProductPortraitMapFunction
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 13:10
 * @Version 1.0
 */
public class ProductPortraitMapFunction extends RichMapFunction<String,String> {

    @Override
    public String map(String value) throws Exception {
        LogEntity log = LogToEntity.getLog(value);
        ResultSet rst = MysqlClient.selectUserById(log.getUserId());
        if (rst != null){
            while (rst.next()){
                String productId = String.valueOf(log.getProductId());
                String sex = rst.getString("sex");
                HbaseClient.increamColumn("prod",productId,"sex",sex);
                String age = rst.getString("age");
                HbaseClient.increamColumn("prod",productId,"age", AgeUtil.getAgeType(age));
            }
        }
        return null;
    }
}
