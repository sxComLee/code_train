package udf;

import com.google.common.io.BaseEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by yqlong on 2017/9/29.
 */
public class UDFBase64EncodeTwice extends UDF {
    public String evaluate(String text) {
        if (StringUtils.isEmpty(text)) {
            return null;
        }
        String firstEncode = BaseEncoding.base64().encode(text.getBytes());
        String secondEncode = BaseEncoding.base64().encode(firstEncode.getBytes());
        return secondEncode;
    }
}
