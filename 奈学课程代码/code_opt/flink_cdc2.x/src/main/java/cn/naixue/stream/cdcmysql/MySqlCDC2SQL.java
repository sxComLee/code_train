package cn.naixue.stream.cdcmysql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
public class MySqlCDC2SQL {
    public static void main(String[] args) throws Exception {
//1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT," +
                " NAME STRING," +
                " age INT ,primary key ( id  ) not enforced" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'bigdata03'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'testdb'," +
                " 'table-name' = 'testUser'" +
                ")");
        tableEnv.executeSql("select * from user_info").print();
        env.execute();
    }
}