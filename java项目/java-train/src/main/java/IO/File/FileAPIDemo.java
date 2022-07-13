package IO.File;

import java.io.File;

/**
 * @ClassName FileAPIDemo
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-20 13:26
 * @Version 1.0
 */
public class FileAPIDemo {
    public static void main(String[] args) {
        String path = "/Users/jiang.li/IdeaProjects/self/learn/java/src/main/java/IO/File/李将-大数据开发.pdf";

        //通过路径构建File对象
        File file = new File(path);

        //基本信息
//        System.out.println("名称"+file.getName());
//        System.out.println("路径"+file.getPath());
//        System.out.println("绝对路径"+file.getAbsolutePath());
//        System.out.println("父路径"+file.getParent());
//        System.out.println("父对象"+file.getParentFile().getName());

        /*
         * 文件状态：
         *  1、不存在，
         *  2、存在
         *      文件 isFile
         *      文件夹 isDirctory
         **/
//        System.out.println("是否存在" + file.exists());
//        System.out.println("是否是文件" + file.isFile());
//        System.out.println("是否是文件夹" + file.isDirectory());
//
//        if (null == file || !file.exists()) {
//            System.out.println("文件不不存在");
//        } else {
//            if (file.isFile()) {
//                System.out.println("文件操作");
//            } else {
//                System.out.println("文件夹操作");
//            }
//        }

        /*
         * 文件字节数 length()
         **/
//        System.out.println("字节数"+file.length());
//        //文件夹没有字节
////        file = file.getParentFile();
//        file = new File("/Users/jiang.li/IdeaProjects/self/learn/java/src/main/java/IO/File");
//        System.out.println("字节数"+file.length());
//        //不存在的文件没有字节数
//        file = new File(file,"as");
//        System.out.println("字节数"+file.length());

        /*
         * 创建文件 createNewFile() ：不存在创建成功，存在创建失败
         */
//        file = new File(file.getParentFile(), "io.txt");
//        try {
//            boolean flag = file.createNewFile();
//            System.out.println("是否创建成功："+flag);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        boolean delete = file.delete();
//        System.out.println("文件删除成功："+delete);

        /*
         * 创建目录 mkdir() ：确保上级目录存在，不存在创建失败
         *          mkdirs():上级目录可以不存在，不存在一同创建
         */


        /*
         * 获取下级名称list()
         */


        /*
         * 获取下级File listFiles()
         */

        /*
         * 获取所有的根路径 listRoots()
         */

    }

}
