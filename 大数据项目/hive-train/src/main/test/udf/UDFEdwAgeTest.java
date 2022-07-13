package udf;

import junit.framework.TestCase;
import org.joda.time.DateTime;

/**
 * Created by yqlong on 2018/1/11.
 */
public class UDFEdwAgeTest extends TestCase {
    private UDFEdwAge edwAge;

    @Override
    protected void setUp() throws Exception {
        edwAge = new UDFEdwAge();
    }

    public void testEvaluate() throws Exception {
        String date1 = "2000-02-01";
        Integer age = edwAge.evaluate(date1);
        if (DateTime.now().isBefore(new DateTime(2018, 2, 1, 0, 0, 0).toInstant()) ) {
            assertEquals(17, age.intValue());
        } else {
            assertEquals(19, age.intValue());
        }

        date1 = "2000-01-01";
        age = edwAge.evaluate(date1);
        assertEquals(19, age.intValue());

        date1 = "2019-01-01";
        age = edwAge.evaluate(date1);
        if (DateTime.now().isBefore(new DateTime(2019, 1, 1, 0, 0, 0).toInstant()) ) {
            assertEquals(0, age.intValue());
        }

        date1 = null;
        age = edwAge.evaluate(date1);
        assertNull(age);
    }

    public void testEvaluate1() throws Exception {
        String date1 = "2018-01-10";
        String date2 = "2000-02-01";
        Integer age = edwAge.evaluate(date1, date2);
        assertEquals(17, age.intValue());
    }
}