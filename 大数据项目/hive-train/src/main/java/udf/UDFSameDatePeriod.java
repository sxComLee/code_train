package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by ru.gao on 2017/8/15.
 */
public class UDFSameDatePeriod extends UDF {


    public boolean evaluate(String date1,String date2,String dateType){

        boolean isSameDate = false;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            format.setLenient(false);
            format.parse(date1);
            format.parse(date2);
        }
        catch (Exception e) {
            return false;
        }

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        String yesterday = new SimpleDateFormat( "yyyy-MM-dd ").format(cal.getTime());

        if (date1 == null || date2 == null || dateType == null || date1.length() < 10 || date2.length() < 10) {
            isSameDate = false;
        } else if ("day".equals(dateType)) {
            isSameDate = date1.substring(0, 10).equals(date2.substring(0, 10));
        } else if ("week".equals(dateType)) {
            isSameDate = isSameWeek(date1, date2)
                    && date1.substring(0, 10).compareTo(yesterday) <= 0;
        } else if ("month".equals(dateType)) {
            isSameDate = date1.substring(0, 7).equals(date2.substring(0, 7))
                    && date1.substring(0, 10).compareTo(yesterday) <= 0;
        } else if ("monthly".equals(dateType)) {
            isSameDate = date1.substring(0, 7).equals(date2.substring(0, 7))
                    && date1.substring(0, 10).compareTo(date2.substring(0, 10)) <= 0;
        } else if ("year".equals(dateType)) {
            isSameDate = date1.substring(0, 4).equals(date2.substring(0, 4))
                    && date1.substring(0, 10).compareTo(yesterday) <= 0;
        } else if ("yearly".equals(dateType)) {
            isSameDate = date1.substring(0, 4).equals(date2.substring(0, 4))
                    && date1.substring(0, 10).compareTo(date2.substring(0, 10)) <= 0;
        } else if ("quarter".equals(dateType)) {
            isSameDate = isSameQuarter(date1, date2)
                    && date1.substring(0, 10).compareTo(yesterday) <= 0;
        } else if ("quarterly".equals(dateType)) {
            isSameDate = isSameQuarter(date1, date2)
                    && date1.substring(0, 10).compareTo(date2.substring(0, 10)) <= 0;
        }

        return isSameDate;

    }




    public static boolean isSameWeek(String date1, String date2)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = null;
        Date d2 = null;
        try
        {
            d1 = format.parse(date1);
            d2 = format.parse(date2);
        }
        catch (Exception e)
        {
            return false;
        }
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();

        cal1.setTime(d1);
        cal2.setTime(d2);
        cal1.setFirstDayOfWeek(Calendar.MONDAY);//将周一设为一周第一天
        cal2.setFirstDayOfWeek(Calendar.MONDAY);
        int subYear = cal1.get(Calendar.YEAR) - cal2.get(Calendar.YEAR);
        int subMonth = cal1.get(Calendar.MONTH) - cal2.get(Calendar.MONTH);
        if (subYear == 0)
        {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR) && Math.abs(subMonth) <= 1)
                return true;
        }
        else if (subYear == 1 && cal1.get(Calendar.MONTH) == 0 && cal2.get(Calendar.MONTH) == 11) //subYear==1,说明cal比cal2大一年;java的一月用"0"标识，那么12月用"11"
        {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR))
                return true;
        }
        else if (subYear == -1 && cal1.get(Calendar.MONTH) == 11 &&  cal2.get(Calendar.MONTH) == 0)
        {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR))
                return true;
        }
        return false;
    }





    public static boolean isSameQuarter(String date1, String date2)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = null;
        Date d2 = null;
        try
        {
            d1 = format.parse(date1);
            d2 = format.parse(date2);
        }
        catch (Exception e)
        {
            return false;
        }
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();

        cal1.setTime(d1);
        cal2.setTime(d2);

        int month1 = cal1.get(Calendar.MONTH) + 1;
        int month2 = cal2.get(Calendar.MONTH) + 1;
        if ( cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) )
        {
            if (month1 >= 1 && month1 <= 3 && month2 >= 1 && month2 <= 3){
                return true;
            }else if(month1 >= 4 && month1 <= 6 && month2 >= 4 && month2 <= 6){
                return true;
            }else if(month1 >= 7 && month1 <= 9 && month2 >= 7 && month2 <= 9){
                return true;
            }else if(month1 >= 10 && month1 <= 12 && month2 >= 10 && month2 <= 12){
                return true;
            }

        }

        return false;
    }




    public static void main(String[] args) {
        UDFSameDatePeriod sObj = new UDFSameDatePeriod();
        boolean a = sObj.evaluate("2017-02-29", "2017-02-29", "day");
        System.out.println(a);

        boolean b = sObj.evaluate("2014-01-01", "2014-12-29", "week");
        System.out.println(b);

        boolean e = sObj.evaluate("2017-08-07 22:12:01", "2017-08-13", "week");
        System.out.println(e);

        boolean f = sObj.evaluate("2017-01-01", "2016-12-31 25:12:61", "week");
        System.out.println(f);

        boolean c = sObj.evaluate("2017-07-31", "2017-08-15", "month");
        //   String date1 = "2017-08-31";
        //   String date2 = "2017-08-15";
        //   System.out.println(date1.substring(0,7));
        //   System.out.println(date2.substring(0,7));
        System.out.println(c);

        boolean d = sObj.evaluate("2016-01-31", "2017-08-15", "year");
        //   String date3 = "2017-08-31";
        //   String date4 = "2017-08-15";
        //   System.out.println(date3.substring(0,4));
        //   System.out.println(date4.substring(0,4));
        System.out.println(d);

        boolean g = sObj.evaluate("2017-07-31", "2017-07-15", "monthly");
        System.out.println(g);

        boolean h = sObj.evaluate("2017-07-15", "2017-07-31", "monthly");
        System.out.println(h);

        boolean i = sObj.evaluate("2017-07-15 22:12:01", "2017-07-15 21:12:01", "monthly");
        System.out.println(i);

        boolean j = sObj.evaluate("2017-01-01 22:12:01", "2017-03-31 21:12:01", "quarter");
        System.out.println(j);

        boolean k = sObj.evaluate("2017-03-01 22:12:01", "2017-01-31 21:12:01", "quarterly");
        System.out.println(k);

        boolean l = sObj.evaluate("2017-01-01", "2017-02-06", "yearly");
        System.out.println(l);

        boolean m = sObj.evaluate("2017-07-29", "2017-07-28", "month");
        System.out.println(m);

        boolean n = sObj.evaluate("2017-08-18", "2017-08-28", "month");
        System.out.println(n);


        boolean o = sObj.evaluate("2016-12-12", "2017-12-10", "week");
        System.out.println(o);

        o = sObj.evaluate("2017-12-08", "2017-12-10", "week");
        System.out.println(o);
    }
}
