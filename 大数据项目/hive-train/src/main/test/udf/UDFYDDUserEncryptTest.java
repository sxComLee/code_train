package udf;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2017/10/19.
 */
public class UDFYDDUserEncryptTest extends TestCase {

    public void testEvaluate() throws Exception {
        UDFYDDUserEncrypt en = new UDFYDDUserEncrypt();
        String encrypted = en.evaluate("abcd");
        System.out.println(encrypted);
        assertEquals("6897qMEEd45RAK0wFyNuPw==", encrypted);
    }
}