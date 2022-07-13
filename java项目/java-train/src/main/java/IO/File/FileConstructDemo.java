package IO.File;

import java.io.File;

/**
 * @ClassName FileConstructDemo
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-20 11:07
 * @Version 1.0
 */
public class FileConstructDemo {
    public static void main(String[] args) {
        String path = "/Users/jiang.li/IdeaProjects/self/learn/java/src/main/java/IO/File/李将-大数据开发.pdf";

        //通过路径构建File对象
        File file = new File(path);
        System.out.println(file.length());

        //通过父类路径构建File对象
//        file= new File("/Users/jiang.li/IdeaProjects/self/learn/java/src/main/java/IO/File/"
//                , "李将-大数据开发.pdf");
        file= new File("/Users/jiang.li/IdeaProjects/self/learn/"
                , "java/src/main/java/IO/File/李将-大数据开发.pdf");
        System.out.println(file.length());

        //通过父类File对象构建
        file = new File(new File("/Users/jiang.li/IdeaProjects/self/learn/java/src/main/java/IO/File")
                ,"李将-大数据开发.pdf");
        System.out.println(file.length());
    }
}
