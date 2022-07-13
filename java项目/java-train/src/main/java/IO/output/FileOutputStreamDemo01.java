package IO.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @ClassName TestDemo01
 * @Description TODO 文件字节输出流
 * @Author jiang.li
 * @Date 2020-01-27 15:37
 * @Version 1.0
 */
public class FileOutputStreamDemo01 {
    public static void main(String[] args) {
        //创建数据源
        File des = new File("des.txt");

        //选择流
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(des,true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //具体处理
        try{
            //准备数据
            String data = "hello world,hello java \r\n";
            //数据写出
            os.write(data.getBytes(), 0, data.length());
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

        //关闭资源

    }

}
