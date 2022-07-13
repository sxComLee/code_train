package udf;

import com.fengjr.edw.utils.AESUtils;
import junit.framework.TestCase;

/**
 * Created by yqlong on 2018/1/11.
 */
public class UDFSaltEncryptTest extends TestCase {

    public void testEvaluate() throws Exception {

        /*
         * 此处使用AES-128-ECB加密模式，key需要为16位。
         */
        String cKey = "1234567890123456";


        // 需要加密的字串
        String cSrc = "www.fengjr.com";
        System.out.println(cSrc);


        UDFSaltEncrypt udfSaltEncrypt = new UDFSaltEncrypt();
        String encryptStr = udfSaltEncrypt.evaluate(cSrc, cKey);
        System.out.println(encryptStr);

        UDFSaltDecrypt udfSaltDecrypt = new UDFSaltDecrypt();
        String decryptStr = udfSaltDecrypt.evaluate(encryptStr,cKey);

        assertEquals(cSrc, decryptStr);

        String decryptStr2 = udfSaltDecrypt.evaluate(encryptStr,"1234567890123451");

        // 加密
        String enString = AESUtils.Encrypt(cSrc, cKey);
        System.out.println("加密后的字串是：" + enString);

        // 解密
        String DeString = AESUtils.Decrypt(enString, cKey);
        System.out.println("解密后的字串是：" + DeString);
    }

}