package udf;

import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by yqlong on 2018/1/5.
 */
public class JodaTimeTest extends TestCase {
    private static final String FORMATE_DATE = "yyyy-MM-dd";
    private static final DateTimeFormatter format = DateTimeFormat.forPattern(FORMATE_DATE);

    public void testWeekTime() throws Exception {
        DateTime dateTime = new DateTime(2017, 1, 1, 23, 59, 59);
        String firstDate = dateTime.dayOfWeek().withMinimumValue().toString(FORMATE_DATE);
        String lastDate = dateTime.dayOfWeek().withMaximumValue().toString(FORMATE_DATE);
        System.out.println(firstDate);
        System.out.println(lastDate);
    }
}
