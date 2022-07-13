package com.jiang.flink.study.source.Sources;

import com.jiang.flink.study.source.model.Student;
import com.jiang.flink.study.source.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName MysqlSource
 * @Description TODO 注意 RichParallelSourceFunction<Student> 和 RichSourceFunction<Student> 的区别
 * @Author jiang.li
 * @Date 2019-12-16 14:15
 * @Version 1.0
 */
public class MysqlSource extends RichSourceFunction<Student> {

    PreparedStatement ps;
    private Connection conn;
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = MysqlUtil.getConnection("com.mysql.jdbc.Driver"
        ,"jdbc:msyql:3306/localhost:3306/test?userUnicode=true&characterEncoding=UFT-8"
        ,"root"
        ,"root");
        String sql = "select * from Student";
        ps = this.conn.prepareStatement(sql);
    }


    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            sourceContext.collect(student);
        }
    }


    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) { //关闭连接和释放资源
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void cancel() {

    }
}
