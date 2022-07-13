package utils;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2018/5/31.
 */
public class PhoneNumberUtilTest extends TestCase {

    public void testRemove86() throws Exception {
        assertEquals("17600879306", PhoneNumberUtil.remove86("8617600879306"));
        assertEquals("17600879306", PhoneNumberUtil.remove86("+8617600879306"));
        assertEquals("17600879306", PhoneNumberUtil.remove86("17600879306"));
    }
}