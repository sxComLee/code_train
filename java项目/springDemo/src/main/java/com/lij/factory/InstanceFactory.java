package com.lij.factory;

import com.lij.service.IAccountService;
import com.lij.service.impl.AccountServiceImpl;

public class InstanceFactory {
    public IAccountService getAccountService(){
        return new AccountServiceImpl();
    }
}
