package com.lij.ui;

import com.lij.dao.IAccountDao;
import com.lij.factory.BeanFactory;
import com.lij.service.IAccountService;
import com.lij.service.impl.AccountServiceImpl;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 模拟一个表现层，用于调用业务层
 */
public class Client {
    /**
     * 获取spring的Ioc核心容器，并根据id获取对象
     *  applicationcontext的实现类
     *      ClasspathXmlApplicationContext
     *      FileSystemXmlApplicationContext
     *      AnnotationConfigApplicationContext
     *
     *  核心容器的两个接口引发的问题
     *      ApplicationContext: 单例对象适用 ， 采用此接口定义对象
     *          在构建核心容器时，创建对象采取的立即加载的方式
     *      BeanFactory ： 多例对象适用
     *          在构建核心容器时，创建对象采取的延迟加载的方式，什么时候根据id获取对象，什么时候才开始加载
     * @param args
     */
    public static void main(String[] args) {
//        for (int i = 0; i < 5; i++) {
//            IAccountService as = (IAccountService) BeanFactory.getBean("accountService");
//            as.saveAccount();
//        }

//        IAccountService as = new AccountServiceImpl();
//        as.saveAccount();

        //1、获取核心容器对象
        ClassPathXmlApplicationContext ac = new ClassPathXmlApplicationContext("bean.xml");
        //2、根据id获取Bean对象
//        IAccountService as = (IAccountService) ac.getBean("accountService");
//        as = (IAccountService) ac.getBean("accountService2");
        IAccountService  as = (IAccountService) ac.getBean("accountService");
//        IAccountDao ad = ac.getBean("accountDao", IAccountDao.class);
//        IAccountDao ad = ac.getBean("accountDao", IAccountDao.class);
//        System.out.println(as);
//        System.out.println(ad);
        as.saveAccount();

        //手动关闭容器类型
        ac.close();
    }
}
