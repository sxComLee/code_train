package com.lij.rpc.writable.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Description: 模拟 Hadoop 构建一个 RPC 服务端
 *
 * @author lij
 * @date 2021-12-02 07:59
 */
public class MyServer {
    public static void main(String[] args) throws IOException {
        // TODO_lij 注释：构建一个RPC 服务端
        // TODO_lij 注释：服务端，提供了一个 BussinessProtocol 协议的 BusinessIMPL 服务实现
        RPC.Server server = new RPC.Builder(new Configuration())
                .setProtocol(BussinessProtocol.class)
                .setInstance(new BusinessIMPL())
                .setBindAddress("localhost")
                .setPort(6789)
                .build();
        //server 启动
        server.start();
    //输出结果：   成功创建了文件夹 /home/bigdata/apps
        // 成功打了招呼： hello bigdata
    }
}
