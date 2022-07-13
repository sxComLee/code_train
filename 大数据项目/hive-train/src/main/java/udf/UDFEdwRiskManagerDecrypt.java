package udf;

import com.fengjr.locksmith.api.bean.LocksmithResultBean;
import com.fengjr.locksmith.storage.key.Encryption;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * EdwSecret.
 *
 */
public class UDFEdwRiskManagerDecrypt extends UDF {
    String appId = "1c621e3f02f3";
    String salt = "risk-manager";
    public String evaluate(String value){
        if(value == null ||  "".equals(value)){
            return null;
        }
        Encryption es =  EncryptionSingleton.getInstance();
        LocksmithResultBean decryptResult = es.decrypt(appId, salt, value);
        if(decryptResult.isSuccess()){
            return decryptResult.getResult().toString();
        }else{
            return  decryptResult.getCode() + ":" + decryptResult.getMsg();
        }

    }


}
