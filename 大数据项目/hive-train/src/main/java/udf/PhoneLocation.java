package udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * Created by jialiang.li@fengjr.com on 2016/6/13.
 */


public class PhoneLocation extends UDF {
    private final static int DEFAULT_ARRAY_SIZE = 10;
    /**
     * 按照手机号前三位分为10组
     */
    @SuppressWarnings("unchecked")
    private final static HashMap<String, PhoneLocationDto>[] phone_location_info = new HashMap[DEFAULT_ARRAY_SIZE];

    static {
        for (int i = 0; i < phone_location_info.length; i++) {
            phone_location_info[i] = new HashMap<String, PhoneLocationDto>();
        }
        LoadIPLocation();
    }

    private static void LoadIPLocation() {
        Configuration conf = new Configuration();
        String uri = "hdfs://feng-cluster/udf/ip_id/mobile/mobile.txt";
        FileSystem fs = null;
        FSDataInputStream in = null;
        BufferedReader d = null;
        try {
            fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));
            d = new BufferedReader(new InputStreamReader(in));
            String s = null;
            while (true) {
                s = d.readLine();
                if (s == null) {
                    break;
                }
                String[] split = s.split(",");

                PhoneLocationDto infoNode = new PhoneLocationDto();
                infoNode.setID(Integer.parseInt(split[0]));
                infoNode.setMobileNumber(split[1]);
                infoNode.setMobileArea(split[2]);
//                infoNode.setMobileType(split[3]);
//                infoNode.setAreaCode(split[4]);
//                infoNode.setPostCode(split[5]);

                Integer mobileNum = Integer.parseInt(infoNode.getMobileNumber());
                phone_location_info[mobileNum % DEFAULT_ARRAY_SIZE].put(infoNode.getMobileNumber(), infoNode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public String evaluate(String str, String... beans) {
        if ("".equals(str) || str == null)
            return null;
        else {
            try {
                String phoneNum = str;
                if (!StringUtils.isNumeric(phoneNum)) {
                    return null;
                }
                if (phoneNum == null || (phoneNum.length() != 11 && phoneNum.length() != 7)) {
                    return null;
                }
                String strPhoneNumPre7 = phoneNum.substring(0, 7);
                Integer intPhoneNum = Integer.parseInt(phoneNum.substring(0, 7));
                PhoneLocationDto phone = phone_location_info[intPhoneNum % DEFAULT_ARRAY_SIZE].get(strPhoneNumPre7);
                String[] ipArray = phone.getMobileArea().split(" ");
                if (beans == null || beans.length == 0) {
                    return phone.getMobileArea();
                } else if ("prov".equals(beans[0]) && ipArray.length >= 1) {
                    return ipArray[0];
                } else if ("city".equals(beans[0]) && ipArray.length >= 2) {
                    return ipArray[1];
                } else {
                    return phone.getMobileArea();
                }
            } catch (Exception e) {
                return null;
            }
        }
    }

}