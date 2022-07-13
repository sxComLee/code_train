package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by yqlong on 2017/8/28.
 */
public class UDFFirstDateOfPeriod extends UDF {
    private static final String FORMATE_DATE = "yyyy-MM-dd";
    private static final DateTimeFormatter format = DateTimeFormat.forPattern(FORMATE_DATE);

    public String evaluate(String dateStr, String dateType) throws HiveException {
        String firstDate = null;
        if (!(dateType.equals("day") || dateType.equals("week") || dateType.equals("month") ||
                dateType.equals("monthly") || dateType.equals("quarter") || dateType.equals("quarterly") ||
                dateType.equals("year") || dateType.equals("yearly"))) {
            throw new HiveException("日期类型参数不正确, 日期类型：day, week, month, monthly, quarter, quarterly, year, yearly");
        }

        DateTime dateTime = new DateTime(9999, 12, 31, 23, 59, 59);
        if (dateStr.length() < 10) {
            throw new HiveException("日期格式不正确: " + dateStr + ", 正确格式:YYYY-MM-DD");
        }
        try {
            dateTime = DateTime.parse(dateStr.substring(0, 10));
        } catch (Exception e) {
            throw new HiveException("日期格式不正确: " + dateStr + ", 正确格式:YYYY-MM-DD");
        }

        switch (dateType) {
            case "day":
                firstDate = dateStr.substring(0, 10);
                break;
            case "week":
                firstDate = dateTime.dayOfWeek().withMinimumValue().toString(FORMATE_DATE);
                break;
            case "month":
            case "monthly":
                firstDate = dateTime.dayOfMonth().withMinimumValue().toString(FORMATE_DATE);
                break;
            case "year":
            case "yearly":
                firstDate = dateTime.dayOfYear().withMinimumValue().toString(FORMATE_DATE);
                break;
            case "quarter":
            case "quarterly":
                firstDate = getFirstDateOfQuarterByDate(dateTime).toString(FORMATE_DATE);
                break;
        }
        return firstDate;
    }


    public DateTime getFirstDateOfQuarterByDate(DateTime dateTime) {
        DateTime firstDateOfQ1 = new DateTime(dateTime.getYear(), 1, 1, 0, 0);
        DateTime firstDateOfQ2 = new DateTime(dateTime.getYear(), 4, 1, 0, 0);
        DateTime firstDateOfQ3 = new DateTime(dateTime.getYear(), 7, 1, 0, 0);
        DateTime firstDateOfQ4 = new DateTime(dateTime.getYear(), 10, 1, 0, 0);

        int month = dateTime.getMonthOfYear();
        final DateTime[] dateArr = {firstDateOfQ1, firstDateOfQ1, firstDateOfQ1,
                firstDateOfQ2, firstDateOfQ2, firstDateOfQ2,
                firstDateOfQ3, firstDateOfQ3, firstDateOfQ3,
                firstDateOfQ4, firstDateOfQ4, firstDateOfQ4};
        return dateArr[month - 1];
    }

    public static void main(String[] args) {
        String dateStr = new DateTime().plusDays(18).plusMonths(1)
                .dayOfWeek().withMinimumValue().toString("yyyy-MM-dd HH:mm:ss");
        System.out.println(dateStr);

        String dateStr2 = "2017-01-01 11:11:08.0";
        DateTime date = DateTime.parse(dateStr2.substring(0,10));
        date.dayOfWeek();
        String string_u = date.toString();

        UDFFirstDateOfPeriod fdp = new UDFFirstDateOfPeriod();

//        //day
//        System.out.println(fdp.evaluate("2017-08-29", "day"));
//        //week
//        System.out.println(fdp.evaluate("2017-08-29", "week"));
//        //week of two years
//        System.out.println(fdp.evaluate("2017-01-01", "week"));
//        //month
//        System.out.println(fdp.evaluate("2017-08-29", "month"));
//
//        //monthly
//        System.out.println(fdp.evaluate("2017-08-29", "monthly"));
//
//        //quarter
//        System.out.println(fdp.evaluate("2017-08-29", "quarter"));
//
//        //quarterly
//        System.out.println(fdp.evaluate("2017-08-29", "quarterly"));
//
//        //year
//        System.out.println(fdp.evaluate("2017-08-29", "year"));
//
//        //yearly
//        System.out.println(fdp.evaluate("2017-08-29", "yearly"));
//
//        //invalid date format
//        System.out.println(fdp.evaluate("2017-13-29", "yearly"));
//        System.out.println(fdp.evaluate("2017-08", "yearly"));
//
//        //invalid date type
//        System.out.println(fdp.evaluate("2017-08-29", "asdfjk"));
    }
}
