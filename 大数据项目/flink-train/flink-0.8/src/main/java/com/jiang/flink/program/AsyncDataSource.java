package com.jiang.flink.program;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.template.FlinkFromKafkaModule;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.jiang.flink.program.Constant.*;

/**
 * @ClassName AsyncDataSource
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-21 17:57
 * @Version 1.0
 */
public class AsyncDataSource extends FlinkFromKafkaModule {

    @Override
    public SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource, StreamExecutionEnvironment env) {
//        AsyncDataStream.orderedWait()
        SingleOutputStreamOperator<Tuple3<String, String, String>> deal = kafkaSource.flatMap(new FlatMapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String, String, String>> collector) throws Exception {
                JSONObject valueJson = JSONObject.parseObject(s);

                String data = valueJson.getString(BINLOG_JSON_DATA_FIELD);

                String table = valueJson.getString(BINLOG_JSON_TABLENAME_FIELD);
                JSONArray array = JSONArray.parseArray(data);
                for (int i = 0; i < array.size(); i++) {

                    JSONObject dataJSON = array.getJSONObject(i);

                    String createTime = dataJSON.getString(BINLOG_JSON_DATA_CREATE_FIELD);

                    collector.collect(new Tuple3<>(table, createTime, dataJSON.toJSONString()));
                }
            }
        });

        SingleOutputStreamOperator<String> asynvStream;
        //构建异步查询
        if (true) {
            asynvStream = AsyncDataStream.orderedWait(deal, new MysqlAsyncFunction("","","","",""), 100000L, TimeUnit.MILLISECONDS, 20);
        } else {
            asynvStream = AsyncDataStream.unorderedWait(deal, new MysqlAsyncFunction("","","","",""), 100000L, TimeUnit.MILLISECONDS, 20);

        }

        return null;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 异步查询
     * @Date 16:20 2019-12-12
     * @Param
     * @return
     **/
    class MysqlAsyncFunction extends RichAsyncFunction<Tuple3<String, String, String>, String> {
        private static final String configPath = "config.properties";
        private Cache<String, String> cache;

        private Connection conn;
        private PreparedStatement ps;
        private Boolean isRunning;
        private String sql;
        private String ip;
        private String dbName;
        private String userName;
        private String passWord;

        public MysqlAsyncFunction(String sql, String ip, String dbName, String userName, String passWord) {
            this.sql = sql;
            this.ip = ip;
            this.dbName = dbName;
            this.userName = userName;
            this.passWord = passWord;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //配置缓存
            cache = CacheBuilder
                    .newBuilder()
                    .maximumSize(1025)
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build();

            Class.forName("com.mysql.jdbc.Driver");
            String dbURL = "jdbc:mysql://" + ip + ":3306/" + dbName + "?tinyInt1isBit=false";

            conn = DriverManager.getConnection(dbURL, userName, passWord);
//            ps = conn.prepareStatement("SELECT uid,type FROM bbs_user_info WHERE type<>4");
            ps = conn.prepareStatement(sql);
            isRunning = true;

        }

        //查询逻辑
        @Override
        public void asyncInvoke(Tuple3<String, String, String> stringStringStringTuple3, ResultFuture<String> resultFuture) throws Exception {

        }

        @Override
        public void timeout(Tuple3<String, String, String> input, ResultFuture<String> resultFuture) throws Exception {

        }

        @Override
        public void close() throws Exception {
            super.close();
            if(ps != null){
                ps.close();
            }
            if(conn != null){
                conn.close();
            }
        }
    }

}
