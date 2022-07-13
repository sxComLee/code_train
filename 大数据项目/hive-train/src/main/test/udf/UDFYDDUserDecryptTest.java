package udf;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2017/10/19.
 */
public class UDFYDDUserDecryptTest extends TestCase {

    public void testEvaluate() throws Exception {
        UDFYDDUserDecrypt decrypt = new UDFYDDUserDecrypt();
        String s = decrypt.evaluate("6897qMEEd45RAK0wFyNuPw==");
        assertEquals("abcd", s);
    }
}