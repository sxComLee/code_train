package org.naixue.mazh.flink1142.source.user_define;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义数据源，从 mysql 中读取数据
 */
public class UserDefineSource_FromMySQL_Main {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<Student> studentDS = executionEnvironment.addSource(new MySQLSource()).setParallelism(1);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<SexCount> resultDS = studentDS.map(new MapFunction<Student, SexCount>() {
            @Override
            public SexCount map(Student student) throws Exception {
                System.out.println("接收到一条学生数据：" + student.toString());
                return new SexCount(student.getSex(), 1);
            }
        });

        // TODO_MA 马中华 注释：
        resultDS.print().setParallelism(1);

        // TODO_MA 马中华 注释：
        executionEnvironment.execute("UserDefineSource_FromMySQL_Main");
    }
}



/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义一个数据源，有多种方式
 *  有两种方式：
 *  1、implements SourceFunction
 *  2、extends RichSourceFunction  更富有： 功能跟强大， 提供一些生命周期方法
 */
class MySQLUDSource extends RichSourceFunction<Student>{

    // TODO_MA 马中华 注释： 当初始化 MySQLUDSource 这个实例之后，会立即执行 open 一次，
    // TODO_MA 马中华 注释： 做初始化：链接，状态恢复，重量级大对象等等
    @Override
    public void open(Configuration parameters) throws Exception {

    }

    // TODO_MA 马中华 注释： 正儿八经的逻辑
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {

    }

    // TODO_MA 马中华 注释： 最后执行一次
    @Override
    public void cancel() {

    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 实现的逻辑效果： 每隔 1s 读取到一条数据输出到 FLink 程序中执行计算
 */
class MySQLSource extends RichSourceFunction<Student> {

    private boolean isRunning = true;

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://bigdata02:3306/studentdb?useSSL=false";
        String username = "root";
        String password = "QWer_1234";
        String sql = "select id, name, sex, age, department from student;";

        // TODO_MA 马中华 注释： 初始化链接
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        preparedStatement = connection.prepareStatement(sql);
    }

    // TODO_MA 马中华 注释： 逻辑效果： 每隔 1s 输出一条记录
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        // TODO_MA 马中华 注释： 执行 SQL
        ResultSet resultSet = preparedStatement.executeQuery();
        while (isRunning & resultSet.next()) {

            // TODO_MA 马中华 注释： 读取数据
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String sex = resultSet.getString("sex");
            int age = resultSet.getInt("age");
            String department = resultSet.getString("department");
            // TODO_MA 马中华 注释： 组装成一条数据
            Student student = new Student(id, name, sex, age, department);

            // TODO_MA 马中华 注释： 发送数据到下游 Operator
            sourceContext.collect(student);

            // TODO_MA 马中华 注释： 每隔 1s 钟输出一条数据
            Thread.sleep(1000);
        }

        /**
         // 效果：不停的每隔 1s 钟读取 大小为 100 的一批次数据
         // TODO_MA 马中华 注释： 数据结构和算法！
         while (isRunning){
            min = 1
            max = 100
            step = 100
            // select id, name, sex, age, department from student where id > min and id < max
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
               // 处理
                // 输出
            }
            min += step;
            max += step
            Thread.sleep(1000);
         }

         */
    }

    @Override
    public void cancel() {
        isRunning = false;
        System.out.println("程序取消了");
    }
}

class Student {
    private int id;
    private String name;
    private String sex;
    private int age;
    private String department;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public Student() {
    }

    public Student(int id, String name, String sex, int age, String department) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.department = department;
    }

    @Override
    public String toString() {
        return "Student{" + "id=" + id + ", name='" + name + '\'' + ", sex='" + sex + '\'' + ", age=" + age + ", department='" + department + '\'' + '}';
    }
}

class SexCount {
    private String sex;
    private int count;

    public SexCount() {
    }

    public SexCount(String sex, int count) {
        this.count = count;
        this.sex = sex;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "SexCount{" + "sex='" + sex + '\'' + ", count=" + count + '}';
    }
}