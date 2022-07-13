package com.lij.rpc.writable.demo1;

/**
 * Description:
 *  定义一个 RPC BussinessProtocol 通信协议的服务实现组件
 * @author lij
 * @date 2021-12-02 07:37
 */
public class BusinessIMPL implements BussinessProtocol{
    @Override
    public void mkdir(String path) {
        System.out.println("成功创建了文件夹 "+ path);
    }

    @Override
    public String getName(String name) {
        System.out.println("成功打了招呼： hello "+ name);
        return "bigdata-lij";
    }
}
