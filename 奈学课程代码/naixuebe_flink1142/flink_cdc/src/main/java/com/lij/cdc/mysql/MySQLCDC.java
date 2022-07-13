package com.lij.cdc.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-22 11:44
 */
public class MySQLCDC {
    public static void main(String[] args) {
    //    通过flink cdc 的程序直接捕获mysql 实时产生的数据
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //    只需要获取 StreamTableEnvironment
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(localEnvironmentWithWebUI, settings);

        //  定义表结构
        String createTable = "CREATE TABLE mysql_binlog_test" +
                "(id INT , name STRING , age INT)"+
                " WITH (" +
                ", 'connector' = 'mysql-cdc' " +
                ", 'hostname' = 'localhost' " +
                ", 'port' = '3306' " +
                ", 'username' = 'root' " +
                ", 'password' = 'root' " +
                ", 'database-name' = 'test' " +
                ", 'table-name' = 't_user_info' " +
                ")";

        //  使用sql的方式来直接定义表结构，也可以通过代码来操作
        streamTableEnvironment.executeSql(createTable);

        String queryTable = "select * from mysql_binlog_test";

        Table result = streamTableEnvironment.sqlQuery(queryTable);

        // streamTableEnvironment.toRetractStream(result,)


    }
}
