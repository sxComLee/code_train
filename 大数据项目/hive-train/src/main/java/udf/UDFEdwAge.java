package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ru.gao on 2017/8/15.
 */
public class UDFEdwAge extends UDF {

    private final static Logger logger = LoggerFactory.getLogger(UDFEdwAge.class);
    public Integer evaluate(String dateStr){
        if (dateStr == null) {
            return null;
        }
        DateTime date1 = DateTime.now();
        DateTime date2 = null;
        try {
            date2 = DateTime.parse(dateStr.substring(0,  10));
        } catch (Exception e) {
            return null;
        }

        return age(date1, date2);
    }

    public Integer evaluate(String dateStr1, String dateStr2){
        if (dateStr1 == null || dateStr2 == null) {
            return null;
        }
        DateTime date1 = null;
        DateTime date2 = null;
        try {
            date1 = DateTime.parse(dateStr1.substring(0, 10));
            date2 = DateTime.parse(dateStr2.substring(0, 10));
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }

        return age(date1, date2);
    }
    public static Integer age(DateTime date1, DateTime date2){
        if (date1 == null || date2 == null) {
            return null;
        }
        int age = 0;
        int year1 = date1.getYear();
        int month1 = date1.getMonthOfYear();
        int day1 = date1.getDayOfMonth();

        int year2 = date2.getYear();
        int month2 = date2.getMonthOfYear();
        int day2 = date2.getDayOfMonth();

        if (year1 < year2){
            return 0;
        }
        age = year1 - year2;
        if (month1 < month2){
            return age - 1;
        } else if (month1 == month2) {
            if (day1 >= day2) {
                return age;
            } else {
                return age - 1;
            }
        }

        return age;
    }
}
