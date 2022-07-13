package org.lij.flink1_14_2.day02.source.user_Define.defineSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.lij.flink1_14_2.bean.Student;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Description:
 *  实现的逻辑效果： 每隔 1s 读取到一条数据输出到 FLink 程序中执行计算
 *  注释： 自定义一个数据源，有多种方式
 *  有两种方式：
 *  1、implements SourceFunction
 *  2、extends RichSourceFunction  更富有： 功能跟强大， 提供一些生命周期方法
 * @author lij
 * @date 2022-01-29 16:22
 */

public class MysqlSQLSource extends RichSourceFunction<Student> {
    private boolean isRunning = true;

    private Connection connection;
    private PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false";
        String userName = "root";
        String password = "";
        String sql = "select id, name, sex, age, department from student";

        Class.forName(driver);
        connection = DriverManager.getConnection(url, userName, password);
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
