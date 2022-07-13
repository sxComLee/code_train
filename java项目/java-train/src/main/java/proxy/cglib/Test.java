package proxy.cglib;

/**
 * @ClassName Test
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-16 10:59
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
        Singer singer = new Singer();
        Singer singer1 = (Singer) new ProxyFactory(singer).getProxyInstance();
        singer1.sing();
    }

}
