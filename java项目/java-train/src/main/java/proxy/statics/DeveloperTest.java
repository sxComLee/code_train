package proxy.statics;

/**
 * @ClassName DeveloperTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-15 16:23
 * @Version 1.0
 */
public class DeveloperTest {
    public static void main(String[] args) {
        IDeveloper deve = new Developer("jiang");
        deve.writeCode();

        IDeveloper developer = new DeveloperProxy(deve);
        developer.writeCode();
    }

}
