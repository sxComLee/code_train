package IO.input;

import java.io.*;

/**
 * @ClassName test
 * @Description TODO
 *  字节输入流和字符输入流的区别：
 *      共同点是都是通过标准步骤进行操作
 *      区别在于字符借助数组进行操作时用的时char数组，而字节通过byte数组
 * @Author jiang.li
 * @Date 2020-01-27 11:10
 * @Version 1.0
 */
public class test {
    public static void main(String[] args) throws FileNotFoundException {
        //读取数据源
        File file = new File("abc.txt");

        //选择流
        InputStream is = new FileInputStream(file);
//        new ByteInputStream()

        //进行处理
        int tmp;
        try {
            while (true) {

                if (!((tmp = is.read()) != -1)) {
                    break;
                }

                System.out.print((char) tmp);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(is != null){
                //关闭流
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
