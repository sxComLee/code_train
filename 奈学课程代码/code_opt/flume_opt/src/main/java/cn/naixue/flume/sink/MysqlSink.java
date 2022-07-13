package cn.naixue.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlSink extends AbstractSink implements Configurable {
    /**
     * 链接mysql常用的一些参数都从外部给传入过来
     * 链接的url，用户名密码，哪个表，有哪些字段等等
     */

    private Connection connection;
    private Statement stmt;
    private String columnName;
    private String url;
    private String user;
    private String password;
    private String tableName;





    /**
     * 核心的方法，主要是不断地执行，将数据写入到目的地mysql里面去
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        //自定义sink，数据都是在channel里面了，需要先从channel里面去获取到数据
        //所有的数据都是包装成为一个个的Event对象
        Channel ch = getChannel();
        //开启事务的机制，保证数据要么全部sink成功，要么全部sink失败

        Transaction txn = ch.getTransaction();
        Event event =  null;

        txn.begin();

        while(true){
            event = ch.take();
            //如果获取到的event不为空，说明获取到了数据，跳出死循环，去处理数据
            if(event != null){
                break;
            }
        }

        try {
            //获取到的数据，转换成为字符串
            String rawBody = new String(event.getBody());
            //直接将数据保存到mysql里面去，解析字符串，字符串使用\t来进行切割
            String body = rawBody.split("\t")[2];
            if(body.split(",").length == columnName.split(",").length){
                String sql = "insert into " +  tableName + "(" +columnName +  ")  values  ( " + body + " ) " ;
                stmt.executeUpdate(sql);
                //如果处理没有问题，就提交数据
                txn.commit();
                return  Status.READY;
            }else{
                txn.rollback();
                return null;
            }
        } catch (Throwable th) {
            txn.rollback();
            th.printStackTrace();
        }finally {
            txn.close();
        }


        return Status.READY;
    }

    /**
     * 从外部获取配置文件
     * @param context
     */
    @Override
    public void configure(Context context) {
        columnName = context.getString("column_name");
        url = context.getString("url");
        user = context.getString("user");
        password = context.getString("password");
        tableName = context.getString("tableName");

    }

    @Override
    public synchronized void stop() {
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        super.stop();
    }

    @Override
    public synchronized void start() {
        //初始化mysql的链接
        try {
            connection = DriverManager.getConnection(url,user,password);
            stmt = connection.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        super.start();
    }
}
