package com.jiang.flink.program;

import com.jiang.flink.study.common.model.MetricEvent;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import com.jiang.flink.study.source.model.Rule;
import com.jiang.flink.study.source.utils.MysqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName ShceduleAlert
 * @Description TODO 定时读取告警规则
 * @Author jiang.li
 * @Date 2019-12-16 14:33
 * @Version 1.0
 */
@Slf4j
public class ShceduleAlert {
    public static List<Rule> rules;

    public static void main(String[] args) throws Exception {
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);

        threadPool.scheduleAtFixedRate(new GetRulesJob(),0,1, TimeUnit.MINUTES);

        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> source = KafkaConfigUtil.buildSource(env);

        source.map(new MapFunction<MetricEvent, MetricEvent>() {
            @Override
            public MetricEvent map(MetricEvent value) throws Exception {
                if (rules.size() <= 2) {
                    System.out.println("===========2");
                } else {
                    System.out.println("===========3");
                }
                return value;
            }
        });
    }


    static class GetRulesJob implements Runnable {
        @Override
        public void run() {
            try {
                rules = getRules();
            } catch (SQLException e) {
                log.error("get rules from mysql has an error {}", e.getMessage());
            }
        }
    }


    private static List<Rule> getRules() throws SQLException {
        System.out.println("-----get rule");
        String sql = "select * from rule";

        Connection connection = MysqlUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "root123456");

        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        List<Rule> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(Rule.builder()
                    .id(resultSet.getString("id"))
                    .name(resultSet.getString("name"))
                    .type(resultSet.getString("type"))
                    .measurement(resultSet.getString("measurement"))
                    .threshold(resultSet.getString("threshold"))
                    .level(resultSet.getString("level"))
                    .targetType(resultSet.getString("target_type"))
                    .targetId(resultSet.getString("target_id"))
                    .webhook(resultSet.getString("webhook"))
                    .build()
            );
        }

        return list;
    }
}
