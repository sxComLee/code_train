package udf;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2018/8/20.
 */
public class UDFOverduePeriodsTest extends TestCase {
    UDFOverduePeriods overduePeriods;

    public void setUp() throws Exception {
        overduePeriods = new UDFOverduePeriods();

    }

    public void testEvaluate() throws Exception {
        String s1 = "2018-06-22";
        String s2 = "2018-01-21";
        int a = overduePeriods.evaluate(s1, s2);
        assertEquals(6, a);
    }

    public void testEvaluate1() throws Exception {
        String s1 = "2018-06-22";
        String s2 = "2018-01-21";

        String b = overduePeriods.evaluate(s1, s2,"overdue");
        assertEquals("M3+", b);
    }
}