package com.jiang.flink.study.sink;

import com.jiang.flink.study.sink.bean.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName MysqlSink
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-07 19:11
 * @Version 1.0
 */
@Slf4j
public class MysqlSink extends RichSinkFunction<Student> {
    PreparedStatement ps;
    private Connection conn;
    private String url;
    private String userName;
    private String passWord;


    public MysqlSink(String url,String userName,String passWord){
        this.url = url;
        this.userName = userName;
        this.passWord = passWord;
    }

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        if(conn != null){
            ps = this.conn.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭链接和释放资源
        if(ps != null){
            ps.close();
        }
        if(conn != null){
            conn.close();
        }

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Student value, Context context) throws Exception {
        if(ps == null){
            return ;
        }
        //组装数据
        ps.setInt(1,value.getId());
        ps.setString(2,value.getName());
        ps.setString(3,value.getPassword());
        ps.setInt(4,value.getAge());
        ps.execute();
    }

    private Connection getConnection(){
        Connection con = null;
        try{
            Class.forName("com.msyql.jdbc.Driver");

            con = DriverManager.getConnection(url,userName,passWord);
        }catch (Exception e){
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
