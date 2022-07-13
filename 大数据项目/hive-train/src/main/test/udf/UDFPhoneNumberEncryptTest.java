package udf;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2018/1/11.
 */
public class UDFPhoneNumberEncryptTest extends TestCase {
    private UDFPhoneNumberEncrypt udfPhoneNumberEncrypt;

    @Override
    protected void setUp() throws Exception {
        udfPhoneNumberEncrypt=new UDFPhoneNumberEncrypt();
    }

    public void testNum() throws Exception {

        String p = udfPhoneNumberEncrypt.evaluate("17600879888");
        assertEquals("176****9888",p);

        String p1 = udfPhoneNumberEncrypt.evaluate("02442649123");
        assertEquals("024****9123",p1);

        String p2 = udfPhoneNumberEncrypt.evaluate("2649123");
        assertEquals("26***23",p2);
        p2 = udfPhoneNumberEncrypt.evaluate("03702649123");
        assertEquals("037****9123",p2);

        String p3 = udfPhoneNumberEncrypt.evaluate("4008308300");
        assertEquals("4008308300",p3);

        String p4 = udfPhoneNumberEncrypt.evaluate("95599");
        assertEquals("95599",p4);

        String p5 = udfPhoneNumberEncrypt.evaluate("106906199604916");
        assertEquals("106906199604916",p5);

        String p6 = udfPhoneNumberEncrypt.evaluate("8617600879888");
        assertEquals("86176****9888",p6);

        String p7 = udfPhoneNumberEncrypt.evaluate("4008308");
        assertEquals("40***08",p7);

        p6 = udfPhoneNumberEncrypt.evaluate("+8617600879888");
        assertEquals("+86176****9888",p6);

        p6 = udfPhoneNumberEncrypt.evaluate(null);
        assertNull(p6);

        p6 = udfPhoneNumberEncrypt.evaluate("");
        assertEquals("",p6);

        p6 = udfPhoneNumberEncrypt.evaluate("40081234");
        assertEquals("400***34",p6);
    }
}