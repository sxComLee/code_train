package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by yqlong on 2017/9/18.
 */
public class UDFYDDUserEncrypt extends UDF {

    public String evaluate(String str) {

        try {

            return YDDAESUtils.encrypt(str);

        } catch (Exception e) {

            return null;

        }

    }
}
