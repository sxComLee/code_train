package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by yqlong on 2017/1/13.
 */
public class UDFDspUserKeyword extends UDF {
            private final static Logger logger = LoggerFactory.getLogger(UDFDspUserKeyword.class);

        public static String getKeyword(String text) throws UnsupportedEncodingException {
            if (text == null || text.trim().length() == 0) {
                return null;
            }
            String textCopy = text;
            try {
                textCopy = URLDecoder.decode(text, "UTF-8");
            } catch (Exception e) {
                logger.error("decode 错误", e);
            }

            String[] cps = textCopy.split(":", 2);
            if (cps.length <= 1) {
                return null;
            }

            String cpsValue = cps[1];
            String[] cpsKeyPair = cpsValue.split("&");
            for (String keyValuePair : cpsKeyPair) {
                if (keyValuePair.startsWith("mk=") && keyValuePair.length() >= 3) {
                    String value = keyValuePair.substring(3);
                    String[] vals = value.split("[|]");
                    if (vals.length < 3 || vals[1].equals("") || vals[1].length() >= 32 || vals[1].contains("�")) {
                        return null;
                    }

                    return vals[1];
                }
            }

            return null;
        }

        public String evaluate(String text) {
            try {
                return UDFDspUserKeyword.getKeyword(text);
            } catch (UnsupportedEncodingException e) {
                logger.error("UnsupportedEncodingException", e);
            } catch (Exception e) {
                logger.error("Exception", e);
            }

            return null;
        }

    public static void main(String[] args) throws UnsupportedEncodingException {
        UDFDspUserKeyword pk = new UDFDspUserKeyword();
        String text = "CPS_200016-1-1-2-1_200016_0:t=1483838287306&cu=nocu&mk=nomk|10万元如何理财|www.baidu.com/baidu.php";
        String keyword = pk.getKeyword(text);
        System.out.println(keyword);

        text = "CPS_200016-1-1-2-1_200016_0%3At%3D1483838287306%26cu%3Dnocu%26mk%3Dnomk%7C10%E4%B8%87%E5%85%83%E5%A6%82%E4%BD%95%E7%90%86%E8%B4%A2%7Cwww.baidu.com/baidu.php";
        keyword = pk.getKeyword(text);
        System.out.println(keyword);

        text = "CPS_200016-3648_200016_15:t=1480729263743&cu=nocu&mk=nomk|年化收益率6%怎么算|m.baidu.com/baidu.php";
        keyword = pk.getKeyword(text);
        System.out.println(keyword);

        text = "CPS_200016-1-2-1-1_200016:t=1469770993349&cu=nocu&mk=nomk|m6bGo9KsbbQAmBrWvL7uE050yHhGg+mNf3bpDTepm1qL3AlErqWMJUbvcKM5JNTBxH63YXmHPlN7eLWH5IXHEg|m.baidu.com/s";
        keyword = pk.getKeyword(text);
        System.out.println(keyword);

        text = "CPS_200016-1-1-1-1_200016:t%3D1470814703421%26cu%3Dnocu%26mk%3Dnomk%7C%EF%BF%BD%EF%BF%BD%EF%BF%BD%D0%B4%EF%BF%BD%D2%B5%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%7Cshangjia.baidu.com%2Fs";
        keyword = pk.getKeyword(text);
        System.out.println(keyword);

        text = "CPS_1bdis4u1o-i7a_200016_148:t=1504684326274&cu=nocu&mk=nomk:-61045062465:-:-:-:-:-:-cpc|理财产品排行|www.baidu.com/baidu.php&mt=0";
        keyword = pk.getKeyword(text);
        System.out.println(keyword);
    }
}
