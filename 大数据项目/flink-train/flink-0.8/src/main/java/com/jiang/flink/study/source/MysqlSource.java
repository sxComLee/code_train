package com.jiang.flink.study.source;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.jiang.flink.program.bean.RegionInfo;

/**
 * @ClassName MysqlSource
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-15 10:36
 * @Version 1.0
 */
public class MysqlSource extends RichSourceFunction<RegionInfo> {

    private Connection conn ;
    private PreparedStatement ps;
    private Boolean isRunning ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driverClass = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://localhost:3306/flink";
        String userName = "root";
        String passWord = "1234";
        conn = DriverManager.getConnection(dbURL, userName, passWord);
        ps = conn.prepareStatement("select region,value,inner_code from event_mapping ");
    }

    @Override
    public void run(SourceContext<RegionInfo> ctx) throws Exception {
        while(isRunning){
            ResultSet resultSet = ps.executeQuery();
            while(resultSet.next()){
                RegionInfo regionInfo = new RegionInfo(resultSet.getString("region"), resultSet.getString("value"), resultSet.getString("inner_code"));
                ctx.collect(regionInfo);
            }
        }
        //休息两分钟
        Thread.sleep(5000 * 60 );
    }
    /**
     * @Author jiang.li
     * @Description //TODO 优雅停止程序的方法
     * @Date 10:43 2019-11-15
     * @Param []
     * @return void
     **/
    @Override
    public void cancel() {

        try{
            super.close();
            if(null != conn){
                conn.close();
            }
            if(null != ps){
                ps.close();
            }
            isRunning = false;
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
