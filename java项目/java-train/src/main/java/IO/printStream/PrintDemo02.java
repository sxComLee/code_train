package IO.printStream;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @ClassName PrintDemo01
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-29 22:16
 * @Version 1.0
 */
public class PrintDemo02 {
    public static void main(String[] args) throws IOException {
        PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream("abd.txt")),true);
        out.println("打印流");
        out.println(true);
//        out.flush();
        out.close();


    }
}
