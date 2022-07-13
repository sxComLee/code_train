package udf;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.security.MessageDigest;

/**
 * Created by chunlei.yang@fengjr.com on 2018/3/6.
 */
public class UDFEdwPhoneMd5 extends UDF {
    /**
     * 获取十六进制字符串形式的MD5摘要
     */
    public static String md5Hex(String src) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update("http://edw.fengjr.com".getBytes());
            md5.update(src.getBytes("UTF8"));
            //  byte[] bs = md5.digest(src.getBytes());
            byte[] bs = md5.digest();
            return new String(new Hex().encode(bs));
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * @param
     * @return
     */
    public String evaluate(String str){
        //str = "+8617600879306";
        if("".equals(str)||str == null)
            return null;
        else
            //去除手机号中的空格
            str = str.replaceAll(" +","");
        //去除+86开头的手机号中的"+86"
        if(str.startsWith("+86") && str.length() >= 14){
            String S;
            S = str.substring(3,str.length());
            return md5Hex(S);
        }
        //去除+86开头的手机号中的"+86"
        if(str.startsWith("86") && str.length() >= 13){
            String S;
            S = str.substring(2,str.length());
            return md5Hex(S);
        }
        else
            return md5Hex(str);
    }

}
