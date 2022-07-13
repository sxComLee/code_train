package IO.input;

import java.io.*;

/**
 * @ClassName TestDemo01
 * @Description TODO
 * 创建源
 * 选择流
 * 具体操作
 * 释放资源
 * @Author jiang.li
 * @Date 2020-01-26 21:11
 * @Version 1.0
 */
public class FileWriterDemo01 {
    public static void main(String[] args) {
        //创建源
        File file = new File("abc.txt");
        //选择流
        Reader is = null;
        try {
            is = new FileReader(file);
            //操作
//            stepByStepRead(is);
//            forRead(is);
            bufferRead(is);

            //释放资源
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(null != is){
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void bufferRead(Reader is) throws IOException{
        //中间数组，用来存放临时的读取数据
        char[] flash = new char[1024];
        //接收长度
        int tmp;
        while((tmp = is.read(flash)) != -1){
            //字节数组到字符串
            String s = new String(flash,0,tmp);
            System.out.print(s);
        }
    }

    /**
     * @Author jiang.li
     * @Description //TODO 循环读取，输出
     * @Date 10:58 2020-01-27
     * @Param [is]
     * @return void
     **/
    public static void forRead(FileInputStream is) throws IOException{
        int tmp;
        while((tmp = is.read())!=-1){
            System.out.print((char)tmp);
        }
    }

    /**
     * @Author jiang.li
     * @Description //TODO 逐字母读取
     * @Date 10:54 2020-01-27
     * @Param [is]
     * @return void
     **/
    public static void stepByStepRead(FileInputStream is) throws IOException{
        //操作
        int read1 = is.read();
        int read2 = is.read();
        int read3 = is.read();
        System.out.println((char) read1);
        System.out.println((char) read2);
        System.out.println((char) read3);
    }
}
