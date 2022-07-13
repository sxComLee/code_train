package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by yqlong on 2017/9/18.
 */
public class UDFYDDUserDecrypt extends UDF {

    public String evaluate(String str) {

        try {

            return YDDAESUtils.decrypt(str);

        } catch (Exception e) {

            return null;

        }

    }
}
