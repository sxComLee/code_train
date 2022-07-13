package com.lij.factory;

import com.lij.service.IAccountService;
import com.lij.service.impl.AccountServiceImpl;

public class StaticFactory {
    public static IAccountService getAccountService(){
        return new AccountServiceImpl();
    }
}
