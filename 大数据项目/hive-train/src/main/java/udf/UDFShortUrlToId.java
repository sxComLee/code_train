package udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by ru.gao on 2017/8/29.
 */
public class UDFShortUrlToId extends UDF {
    /**
     * 短链转为id
     *
     * @param s
     * @return
     */

    public  long evaluate(String s) {

        if (StringUtils.isBlank(s) || s.length() > 6) {
            return 0;
        }
        long id = 0;
        for (int i = 0; i < s.length(); i++) {
            id = id * 62 + dictionaryMap[s.charAt(i)];
        }
        return id;

    }

    private static final char[] dictionary = {'e', 'f', 'v', 'a', 'n', 'w', '6', 'I', 'u', '5', 'Z', 'T', 'L', '0', '9', 'k', 'O', 'R', 'c', 'd', 'P', 'G', 'J', 'N', 'b', 'E', '3', 'S', 'Y', '1', '8', 'o', 'p', 'U', 'F', 'g', 'Q', 'm', 'X', 'H', 'x', 'A', 'z', 'D', 'l', 'B', 'V', '4', 'q', 'i', 't', 'M', '7', 'C', 'W', 'K', 's', 'h', 'y', 'j', '2', 'r'};
    private static final int[] dictionaryMap = new int[255];

    static {
        for (int i = 0; i < dictionary.length; i++) {
            dictionaryMap[dictionary[i]] = i;
        }
    }





    public static void main(String[] args){
        UDFShortUrlToId sObj = new UDFShortUrlToId();
        long id1 = sObj.evaluate("f9j");
        System.out.println(id1);

        long id2 = sObj.evaluate("f9B");
        System.out.println(id2);
    }

}
