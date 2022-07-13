package com.lij.rpc.writable.demo1;

/**
 * Description: 定义一个RPC协议： 用来定义服务
 * 要实现 VersionedProtocol 这个接口： 不同版本的 Server 和 Client 之前是不能进行通信的
 *
 * @author lij
 * @date 2021-12-02 07:33
 */
public interface BussinessProtocol {

    void mkdir(String path);

    String getName(String name);

    long versionID = 345043000L;
}
