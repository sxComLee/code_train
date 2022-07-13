package IO.File;

import java.io.File;

/**
 * @ClassName test
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-20 10:40
 * @Version 1.0
 */
public class SeparatorCharTest {
    public static void main(String[] args) {
        String path = "";
        //分隔符为 /
        System.out.println(File.separatorChar);
        //路径分隔符为 ：
        System.out.println(File.pathSeparatorChar);
    }
}
