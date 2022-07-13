package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by yijun.yuan on 2018/8/20.
 *计算两个日期 月份差、逾期账龄
 * @date1  较大的日期
 * @date2  较小的日期
 * @type   是否计算逾期
 * 
 */
public class UDFOverduePeriods extends UDF {


    public String evaluate(String date1,String date2,String type){

        if(null == type || !"overdue".equals(type)) {
        	return null;
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = null;
        Date d2 = null;
        try {
            format.setLenient(false);
            d1 = format.parse(date1);
            d2 = format.parse(date2);
        }
        catch (Exception e) {
            return null;
        }

        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        
        c1.setTime(d1);
        c2.setTime(d2);
        
        int year1 = c1.get(Calendar.YEAR);
        int year2 = c2.get(Calendar.YEAR);
        int month1 = c1.get(Calendar.MONTH);
        int month2 = c2.get(Calendar.MONTH);
        int day1 = c1.get(Calendar.DAY_OF_MONTH);
        int day2 = c2.get(Calendar.DAY_OF_MONTH);
        
        int rs = day1 > day2 ? ((year1 - year2) * 12 + (month1 - month2 + 1)): ((year1 - year2)*12 + (month1 - month2));
        
        String overdueFlag = null;
        
        if (rs <= 0){
        	overdueFlag = "C";
        }else if(rs == 1) {
        	overdueFlag = "M1";
        }else if(rs == 2) {
        	overdueFlag = "M2";
        }else if (rs == 3) {
        	overdueFlag = "M3";
        }else if(rs >=4 && rs <=6) {
        	overdueFlag = "M3+";
        }else if(rs > 6) {
        	overdueFlag = "M6+";
        }
        return overdueFlag;

    }
    
    public int evaluate(String date1,String date2){

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = null;
        Date d2 = null;
        try {
            format.setLenient(false);
            d1 = format.parse(date1);
            d2 = format.parse(date2);
        }
        catch (Exception e) {
            return -9999;
        }

        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        
        c1.setTime(d1);
        c2.setTime(d2);
        
        int year1 = c1.get(Calendar.YEAR);
        int year2 = c2.get(Calendar.YEAR);
        int month1 = c1.get(Calendar.MONTH);
        int month2 = c2.get(Calendar.MONTH);
        int day1 = c1.get(Calendar.DAY_OF_MONTH);
        int day2 = c2.get(Calendar.DAY_OF_MONTH);
        
        int rs = day1 > day2 ? ((year1 - year2) * 12 + (month1 - month2 + 1)): ((year1 - year2)*12 + (month1 - month2));
        return rs;

    }


    public static void main(String[] args) {
        UDFOverduePeriods sObj = new UDFOverduePeriods();
        String s1 = "2018-06-22";
        String s2 = "2018-01-21";
        int a = sObj.evaluate(s1, s2);
        System.out.println(a);

        String b = sObj.evaluate(s1, s2,"overdue");
        System.out.println(b);

  
    }
}
