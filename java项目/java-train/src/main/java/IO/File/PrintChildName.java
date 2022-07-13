package IO.File;

import java.io.File;

/**
 * @ClassName PrintChildName
 * @Description TODO
 *  输出父路径下所有文件的名称
 * @Author jiang.li
 * @Date 2020-01-20 11:30
 * @Version 1.0
 */
public class PrintChildName {
    public static void main(String[] args) {
        String path = "/Users/jiang.li/IdeaProjects/self/learn/java";
        File file = new File(path);
        isPrint(file);
    }

    public static void isPrint(File file){
        if(null == file || !file.exists()){
            return;
        }
        if (file.isDirectory()){
            File[] files = file.listFiles();
            for (File fil:files) {
                isPrint(fil);
            }
        }

        if(file.isFile()){
            System.out.println(file.getAbsoluteFile());
        }
    }
}
