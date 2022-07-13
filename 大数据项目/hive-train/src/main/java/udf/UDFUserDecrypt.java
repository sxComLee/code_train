package udf;

/**
 * Created by jialiangli on 2015/12/9.
 */

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFUserDecrypt extends UDF {

    public String evaluate(String str) {

        try {
            TextCipher cipher = new DESTextCipher();
            return cipher.decrypt(str);

        } catch (Exception e) {

            return null;

        }

    }

}