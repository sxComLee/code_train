package IO.commonsIO;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * @ClassName FileUtilDemo01
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-30 10:41
 * @Version 1.0
 */
public class FileUtilDemo01 {
    public static void main(String[] args) {
        //判断文件或者文件夹大小
        long l = FileUtils.sizeOf(new File("./"));
        System.out.println(l);

        //列出子孙级
    }

}
