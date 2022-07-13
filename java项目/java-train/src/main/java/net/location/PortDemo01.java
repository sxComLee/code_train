package net.location;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * @ClassName PortDemo01
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-30 15:58
 * @Version 1.0
 */
public class PortDemo01 {
    public static void main(String[] args) throws UnknownHostException {
//        InetSocketAddress port = new InetSocketAddress(InetAddress.getByName(""), 8887);
        InetSocketAddress port = new InetSocketAddress("", 8888);
        System.out.println(port.getAddress());
        System.out.println(port.getHostName());
        System.out.println(port.getHostString());
        System.out.println(port.getPort());
    }

}
