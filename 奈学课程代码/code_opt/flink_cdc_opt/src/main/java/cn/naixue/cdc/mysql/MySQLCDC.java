package cn.naixue.cdc.mysql;

import cn.naixue.cdc.bean.UserBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySQLCDC {
    public static void main(String[] args) throws Exception {
        //通过flink的cdc的程序直接去捕获mysql的实时产生的数据
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();


        //只需要获取一个对象StreamTableEnvironment
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(localEnvironmentWithWebUI, environmentSettings);

        //定义表结构，直接从mysql当中的binlog获取数据
        String createTable = "CREATE TABLE mysql_binlog " +
                "( id INT ,   NAME STRING,  age INT )" +
                " WITH (  'connector' = 'mysql-cdc'" +
                ", 'hostname' = 'bigdata03'" +
                ", 'port' = '3306'" +
                ", 'username' = 'root'" +
                ", 'password' = '123456'" +
                ", 'database-name' = 'testdb'" +
                ", 'table-name' = 'testUser'  )";

        //使用sql的方式来直接定义表结构，也可以通过代码来操作
        tableEnvironment.executeSql(createTable);

        String queryTable = "select * from mysql_binlog";

        Table result = tableEnvironment.sqlQuery(queryTable);
        //将获取的结果给打印出来
        DataStream<Tuple2<Boolean, UserBean>> tuple2DataStream = tableEnvironment.toRetractStream(result, UserBean.class);
        tuple2DataStream.print();//将结果打印出来
        localEnvironmentWithWebUI.execute();


    }

}
