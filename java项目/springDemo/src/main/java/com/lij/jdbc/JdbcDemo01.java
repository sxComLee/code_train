package com.lij.jdbc;

import java.sql.*;

/**
 * 程序的耦合
 *  耦合：程序间的依赖关系
 *      类之间
 *      方法间
 *  解耦：
 *      降低程序间的依赖关系
 *   实际开发中
 *      应该做到编译期不依赖，运行时才依赖
 *   解耦思路
 *      1、使用反射来创建对象，避免使用new关键字
 *      2、通过读取配置文件获取要创建的对象全限定类名
 */
public class JdbcDemo01 {
    public static void main(String[] args) throws Exception {
        //注册驱动
//        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        Class.forName("com.mysql.jdbc.Driver");
        //注册链接

        Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/springtest","root","");

        //获取操作数据库的预处理对象
        PreparedStatement pstm = conn.prepareStatement("select * from account");

        //执行sql，得到结果集
        ResultSet rs = pstm.executeQuery();
        //遍历结果集
        while(rs.next()){
            System.out.println(rs.getString("name"));
        }
        //释放资源
        rs.close();
        pstm.close();
        conn.close();
    }
}
