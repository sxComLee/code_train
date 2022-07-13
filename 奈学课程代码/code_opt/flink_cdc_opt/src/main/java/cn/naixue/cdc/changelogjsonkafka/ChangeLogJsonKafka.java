package cn.naixue.cdc.changelogjsonkafka;

import cn.naixue.cdc.bean.MysqlOrders;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ChangeLogJsonKafka {
    public static void main(String[] args) throws Exception {
        //使用StreamTableEnvironment来实时的捕获mysql的数据
        //通过flink的cdc的程序直接去捕获mysql的实时产生的数据
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT,"8081-8089");

        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();


        //只需要获取一个对象StreamTableEnvironment
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(localEnvironmentWithWebUI, environmentSettings);

        //将mysql的数据映射成为一张表
        String sourceTable = "CREATE TABLE mysql_orders " +
                "(order_id INT," +
                "order_date timestamp(5) , " +
                "customer_name STRING, " +
                "price DECIMAL(10, 2), " +
                "product_id INT, " +
                "order_status BOOLEAN ) " +
                "WITH ('connector' = 'mysql-cdc'," +
                "'hostname' = 'bigdata03'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'testdb', " +
                "'table-name' = 'orders')" ;




        //定义kafka的表，用于将分析统计的结果，给写入到kafka里面去

        String kafkaSinkTable  = "CREATE TABLE kafka_gmv " +
                "(  day_str STRING, gmv DECIMAL(10, 5),primary key (day_str) not enforced )  " +
                "WITH ( 'connector' = 'upsert-kafka'," +
                "'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092', " +
                "'topic' = 'kafka_gmv', " +
                " 'key.format' = 'json' , 'value.format'='json' )";

        String aggregateSQL = "insert into kafka_gmv select date_format(order_date,'yyyy-MM-dd' ) as day_str ,sum(price) as gmv  from mysql_orders where order_status = true group by date_format(order_date,'yyyy-MM-dd')";




        //使用mysql-cdc映射mysql的数据源表
        tableEnvironment.executeSql(sourceTable);
        tableEnvironment.executeSql(kafkaSinkTable);
        tableEnvironment.executeSql(aggregateSQL);
        //定义需要中的聚合的sql

        Table resultTable = tableEnvironment.sqlQuery("select * from mysql_orders");

        DataStream<Tuple2<Boolean, MysqlOrders>> tuple2DataStream = tableEnvironment.toRetractStream(resultTable, MysqlOrders.class);

        tuple2DataStream.print();


        localEnvironmentWithWebUI.execute();


    }

}
