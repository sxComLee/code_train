package com.lij.rpc.protobuf.demo;

/**
 * Description: 该协议的参数 和 返回值，都是由 Protobuf 生成的 Java 对象
 *
 * @author lij
 * @date 2021-12-02 09:40
 */
public interface MyResourceTracker {
    MyRegisterNodeManagerResponseProto registerNodeManager(MyRegisterNodeManagerRequestProto request) throws Exception;
}
