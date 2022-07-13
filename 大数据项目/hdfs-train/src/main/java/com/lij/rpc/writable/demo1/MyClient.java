package com.lij.rpc.writable.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * Description: 构建rpc客户端
 *
 * @author lij
 * @date 2021-12-02 08:08
 */
public class MyClient {
    public static void main(String[] args) {
        try {
            //  获取了服务端中暴露了的服务协议的一个代理。
            //  客户端通过这个代理可以调用服务端的方法进行逻辑处理
            BussinessProtocol proxy = RPC.getProxy(BussinessProtocol.class
                    , BussinessProtocol.versionID
                    , new InetSocketAddress("localhost", 6789)
                    , new Configuration());

            proxy.mkdir("/home/bigdata/apps");

            String rpcResult = proxy.getName("bigdata");
            System.out.println("从 RPC 服务端接收到的 getName RPC 请求的响应结果： " + rpcResult);

        // 输出结果：   从 RPC 服务端接收到的 getName RPC 请求的响应结果： bigdata-lij
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
