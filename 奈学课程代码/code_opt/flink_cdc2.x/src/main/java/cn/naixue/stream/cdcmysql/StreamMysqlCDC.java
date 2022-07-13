package cn.naixue.stream.cdcmysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamMysqlCDC {
    public static void main(String[] args) throws Exception {

        //通过flink的cdc的程序直接去捕获mysql的实时产生的数据
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //设置checkpoin
        environment.enableCheckpointing(3000);

        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置任务关闭需要保留checkPoint
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //重启的策略
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));


        //状态的保存
        environment.setStateBackend(new FsStateBackend("hdfs://bigdata01:8020/flinkStreamCdc"));

        //设置程序运行的用户
        System.setProperty("HADOOP_USER_NAME","hadoop");


        /**
         * 使用stream API实时的捕获mysql变化的数据，需要写代码，用的人不多，建议还是通过sql的方式
         */
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("192.168.52.120")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("testdb")
                .tableList("testdb.testUser")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        //从mysql数据源当中获取数据，然后不做任何处理，直接打印处理
        environment.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource")
                .setParallelism(4)
                .print().setParallelism(1);
        //执行程序
        environment.execute();

    }
}
