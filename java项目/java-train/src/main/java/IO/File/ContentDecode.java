package IO.File;

/**
 * @ClassName ContentDecode
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-20 15:50
 * @Version 1.0
 */
public class ContentDecode {
    public static void main(String[] args) throws Exception{
        String msg = "生命安全重于泰山a";
        byte[] bytes = msg.getBytes();

        //解码：字符串
        String s = new String(bytes, 0, bytes.length, "utf8");
        System.out.println(s);

        //乱码
        //1） 字节数不够
        s = new String(bytes, 0, bytes.length-2, "utf8");
        System.out.println(s);

        //2）字符集不统一
        s = new String(bytes, 0, bytes.length-2, "GBK");
        System.out.println(s);
    }

}
