package com.lij.service.impl;

import com.lij.service.IAccountService;

import java.util.*;

/**
 * 账户的业务层实现
 */
public class AccountServiceImpl3 implements IAccountService {
    public void setMyStrs(String[] myStrs) {
        this.myStrs = myStrs;
    }

    public void setMyList(List<String> myList) {
        this.myList = myList;
    }

    public void setMyMap(Map<String, String> myMap) {
        this.myMap = myMap;
    }

    public void setProp(Properties prop) {
        this.prop = prop;
    }

    private String[] myStrs;
    private List<String> myList;
    private Map<String,String> myMap;
    private Properties prop;

    public void saveAccount() {
        System.out.println("在 AccountServiceImpl 中账户保存了");
        System.out.println(Arrays.toString(myStrs));
        System.out.println(myList);
        System.out.println(myMap);
        System.out.println(prop);
    }




}
