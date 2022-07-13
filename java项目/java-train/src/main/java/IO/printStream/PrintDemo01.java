package IO.printStream;

import java.io.*;

/**
 * @ClassName PrintDemo01
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-29 22:16
 * @Version 1.0
 */
public class PrintDemo01 {
    public static void main(String[] args) throws IOException {
        PrintStream out = System.out;
        out.println("打印流");
        out.println(true);

        out = new PrintStream(new BufferedOutputStream(new FileOutputStream("abd.txt")),true);
        out.println("打印流");
        out.println(true);
//        out.flush();

        //重定向输出端
        System.setOut(out);
        System.out.println("where am i");
        System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out)),true));
        System.out.println("i am here ");

        out.close();
    }
}
