package IO;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @ClassName copyFileDemo01
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-27 16:00
 * @Version 1.0
 */
public class copyFileDemo01 {
    public static void main(String[] args) {
        copyFile("abc.txt","acd.txt");
    }

    public static boolean copyFile(String src, String des) {
        //获取数据源，输出源和输入源
        File inFile = new File(src);
        File outFile = new File(des);

        //选择对应的流
        FileInputStream in = null;
        FileOutputStream out = null;

        //处理流
        try {
            in = new FileInputStream(inFile);
            out = new FileOutputStream(outFile);
            int inTmp;
//            int outTmp;
            while ((inTmp = in.read()) != -1) {
                out.write(inTmp);
                out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭输出流
            if (null != out) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            //关闭输入流
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        return false;
    }
}
