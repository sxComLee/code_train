package IO.output;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * @ClassName TestDemo01
 * @Description TODO 文件字符输出流
 *  两者的区别在于：文件字符输出流增加了append方法，可以直接进行写出
 * @Author jiang.li
 * @Date 2020-01-27 15:37
 * @Version 1.0
 */
public class FileWriterDemo01 {
    public static void main(String[] args) {
        //创建数据源
        File des = new File("des.txt");

        //选择流
        Writer os = null;
        try {
            os = new FileWriter(des,true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //具体处理
        try{
            //准备数据
            String data = "hello world,hello java \r\n";
            /**  数据写出 **/
            //通过字符串写出
            os.write(data, 0, data.length());
            //通过字节数组写出
            os.write(data.toCharArray());
            //通过append方式写出
            os.append(data, 0, data.toCharArray().length);
            //输出流需要刷新
            os.flush();
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(null != os){
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



    }

}
