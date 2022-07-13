package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by chunlei.yang on 2018/1/30.
 */
public class UDFPhoneNumberEncrypt  extends UDF {

    public  String evaluate(String phoneNum){
        if (phoneNum == null) {
            return phoneNum;
        }
        //去除字符串中的空格
        String phoneNum1 = phoneNum.replaceAll(" +","");
        //null、小于5位、400开头、106开头返回原值
        if (phoneNum1 == null || phoneNum1.length() <= 5) {
            return  phoneNum1;
        }
        if (phoneNum1.length() > 8){
            if (phoneNum1.startsWith("400") || phoneNum1.startsWith("106")) {
                return phoneNum1;
            }
        }

        // 大于等于11位电话号码中间加密
        if (phoneNum1.length() >= 11 && !phoneNum1.startsWith("106")) {
            int len;
            String p1;
            String p2;
            String p;
            len = phoneNum1.length();
            p1 = phoneNum1.substring(0,len - 8);
            p2 = phoneNum1.substring(len - 4,len);
            p = p1+"****"+p2;
            return p;
        }
        //号码长度5位到11位之间，中间加密
        if (phoneNum1.length() > 5 && phoneNum1.length() < 11) {
            int len1;
            String p3;
            String p4;
            String p5;
            len1 = phoneNum1.length();
            p3 = phoneNum1.substring(0,len1 - 5);
            p4 = phoneNum1.substring(len1 - 2,len1);
            p5 = p3 + "***" + p4;
            return p5;
        }
    return phoneNum;
    }


}
