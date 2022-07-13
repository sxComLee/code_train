package net.location;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @ClassName IPDemo01
 * @Description TODO IP定位一个节点：计算机，路由，通讯设备都算
 *  getByName:根据域名DNS或者ip地址解析 -----> IP地址
 * @Author jiang.li
 * @Date 2020-01-30 13:11
 * @Version 1.0
 */
public class IPDemo01 {
    public static void main(String[] args) throws UnknownHostException {
        //根绝getLocalHost获取InetAddress对象
        InetAddress addr = InetAddress.getLocalHost();
        System.out.println(addr.getAddress());
        System.out.println(addr.getCanonicalHostName());
        System.out.println(addr.getHostAddress());
        System.out.println(addr.getHostName());
        System.out.println("====================");

        //根据getByName中的域名获取InetAddress对象
        addr = InetAddress.getByName("www.163.com");
        System.out.println(addr.getAddress());
        System.out.println(addr.getCanonicalHostName());
        System.out.println(addr.getHostAddress()); //返回的是163服务器ip 183.201.213.78
        System.out.println(addr.getHostName()); // 输出 www.163.com
        System.out.println("====================");

        //根据getByName中的ip地址获取InetAddress对象
        addr = InetAddress.getByName("183.201.213.78");
        System.out.println(addr.getAddress());
        System.out.println(addr.getCanonicalHostName());
        System.out.println(addr.getHostAddress()); //返回的是163服务器ip 183.201.213.78
        System.out.println(addr.getHostName()); //输出ip而不是域名，如果这ip地址不存在或者DNS服务器
        // 不允许进行IP或者域名的映射，getHostName就直接返回这个IP的地址
    }

}
