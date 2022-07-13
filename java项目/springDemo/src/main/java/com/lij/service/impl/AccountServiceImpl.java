package com.lij.service.impl;

import com.lij.dao.IAccountDao;
import com.lij.dao.impl.AccountDaoImpl;
import com.lij.factory.BeanFactory;
import com.lij.service.IAccountService;

import java.util.Date;

/**
 * 账户的业务层实现
 */
public class AccountServiceImpl implements IAccountService {

    private String name;
    private Integer age;
    private Date birthDay;

    public AccountServiceImpl() {
    }

    public AccountServiceImpl(String name, Integer age, Date birthDay) {
        this.name = name;
        this.age = age;
        this.birthDay = birthDay;
    }

    public void saveAccount() {
        System.out.println("在 AccountServiceImpl 中账户保存了"+name+""+age+""+birthDay);
    }


//    private IAccountDao accountDao = new AccountDaoImpl();
////    private IAccountDao accountDao = (IAccountDao)BeanFactory.getBean("accountDao");
//
//    public AccountServiceImpl(){
//        System.out.println("创建了AccountServiceImpl对象");
//    }
//    public void saveAccount() {
//        //类的对象，每次都会被重新创建
////        int i =1;
////
////        System.out.println(i);
////        accountDao.saveAccount();
////        System.out.println(accountDao);
////        i++;
//
//        System.out.println("在 AccountServiceImpl 中账户保存了");
//    }
//
//    public void init(){
//        System.out.println("对象进行初始化");
//    }
//
//    public void destory(){
//        System.out.println("对象销毁了");
//    }
}
