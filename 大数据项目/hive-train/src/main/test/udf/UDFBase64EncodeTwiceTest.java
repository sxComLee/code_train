package udf;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2017/9/29.
 */
public class UDFBase64EncodeTwiceTest extends TestCase {

    public void testEvaluate() throws Exception {
        UDFBase64EncodeTwice base64Encode = new UDFBase64EncodeTwice();
        String text = "12341234";
        String encoded = base64Encode.evaluate(text);
        System.out.println(encoded);
        assertEquals("TVRJek5ERXlNelE9", encoded);

        text = null;
        encoded = base64Encode.evaluate(text);
        assertNull(encoded);
    }
}