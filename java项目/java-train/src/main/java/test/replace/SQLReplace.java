package test.replace;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Description:
 *
 * @author lij
 * @date 2022-06-23 10:23
 */
public class SQLReplace {
    public static void main(String[] args) throws Exception {
        String beginDate = "20190102";
        String endDate = "20200630";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyymmdd");
        //开始日期
        Date begin = sdf.parse(beginDate);
        //结束日期
        Date end = sdf.parse(endDate);
        Date middle = begin;
        // 运行中的日期
        // while (middle.after(end))
        // do{
        //
        // }
    }
}
