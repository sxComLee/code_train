package com.lij.factory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 一个创建Bean对象的工厂
 * <p>
 * Bean 在计算机英语中，有可重用组建的含义
 * JavaBean ： 用java语言编写的可重用组件（业务层和持久层都可以看成是可重用组件）
 * JavaBean < 实体类（可重用组件的一部分）
 * <p>
 * 创建service和dao对象
 * <p>
 * 第一个就是需要一个配置文件配置service和dao
 * 配置的内容：全限定类名（key=value形式）
 * 第二个，通过读取配置文件中配置的内容，反射创建对象
 * <p>
 * 采用配置文件可以是xml也可以是properties
 */
public class BeanFactory {

    private static Properties props;
    //用来存放对象名和对应创建的单利对象
    private static Map<String,Object> beans;

    //使用静态代码块转换为Properties对象赋值
    static {
        //实例化对象
        props = new Properties();
        beans = new HashMap<String,Object>();
        //获取properties文件的流对象
        //这里不能直接new一个FileInputStream，因为这里的路径是不一定的，相对路径src，如果是一个web项目就没有了，
        //  绝对路径不一定线上和线下的位置一致，最好通过获取类加载器，获取resource中的文件，最后会成为根路径下的文件
        InputStream in = BeanFactory.class.getClassLoader().getResourceAsStream("bean.properties");
        try {
            props.load(in);
            Enumeration keys = props.keys();
            while(keys.hasMoreElements()){
                String key = keys.nextElement().toString();
                String path = props.getProperty(key);
                Object object = Class.forName(path).newInstance();
                beans.put(key,object);
            }
        } catch (Exception e) {
            e.printStackTrace();
            //这是错误，一旦抛出，不再执行
            throw new ExceptionInInitializerError();
        }


    }

//    public static Object getBean(String beanName){
//        Object o = beans.get(beanName);
//        return o;
//    }


//    public static Object getBean(String beanName) {
//
//        Object bean = null;
//        String beanPath = props.getProperty(beanName);
//        try {
//            bean = Class.forName(beanPath).newInstance();
//
//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        return bean;
//    }
}