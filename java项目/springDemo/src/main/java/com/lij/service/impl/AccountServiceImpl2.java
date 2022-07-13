package com.lij.service.impl;

import com.lij.service.IAccountService;

import java.util.Date;

/**
 * 账户的业务层实现
 */
public class AccountServiceImpl2 implements IAccountService {

    private String name;
    private Integer age;
    private Date birthDay;

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public void setBirthDay(Date birthDay) {
        this.birthDay = birthDay;
    }

    public void saveAccount() {
        System.out.println("在 AccountServiceImpl 中账户保存了"+name+""+age+""+birthDay);
    }


}
