package IO.File;

import java.io.File;

/**
 * @ClassName FileConstructDemo
 * @Description TODO
 *  相对路径和绝对路径
 *  1）存在盘符：绝对路径
 *  2）不存在盘符：相对路径
 * @Author jiang.li
 * @Date 2020-01-20 11:07
 * @Version 1.0
 */
public class FilePathDemo {
    public static void main(String[] args) {
        String path = "/Users/jiang.li/IdeaProjects/self/learn/java/src/main/java/IO/File/李将-大数据开发.pdf";

        //通过路径构建File对象
        File file = new File(path);
        System.out.println(file.getAbsolutePath());
        System.out.println(file.getAbsoluteFile());


        System.out.println(System.getProperty("user.dir"));

        file = new File("李将-大数据开发.pdf");
        System.out.println(file.getAbsolutePath());
        System.out.println(file.getAbsoluteFile());
        //创建时给的什么路径就返回什么路径
        System.out.println(file.getPath());
        System.out.println(file.getName());

    }
}
