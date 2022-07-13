package udf;


import junit.framework.TestCase;

/**
 * Created by chunlei.yang on 2018/3/6.
 */
public class UDFEdwPhoneMd5Test extends TestCase {
    private UDFEdwPhoneMd5 udfEdwPhoneMd5;

    @Override
    protected void setUp() throws Exception {
        udfEdwPhoneMd5=new UDFEdwPhoneMd5();
    }

    public void testNum() throws Exception {
        String p = udfEdwPhoneMd5.evaluate("+8617600879888");
        assertEquals("a0a5e83ff571ad61e82a022028d4c403",p);

        String p1 = udfEdwPhoneMd5.evaluate("8617600879888");
        assertEquals("a0a5e83ff571ad61e82a022028d4c403",p1);

        String p2 = udfEdwPhoneMd5.evaluate("17600879888");
        assertEquals("a0a5e83ff571ad61e82a022028d4c403",p2);

        String p3 = udfEdwPhoneMd5.evaluate("86123456");
        assertEquals("5949e969c0f9be07f1447468313e17c1",p3);

    }
}