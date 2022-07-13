package udf;//package udf;
//
//import com.fengjr.edw.utils.IPDataHandler;
//import org.apache.hadoop.hive.ql.exec.UDF;
//
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class IPToCity extends UDF {
//    private static IPDataHandler iph = new IPDataHandler();
//
//    public String evaluate(String str, String... beans) {
//        if ("".equals(str) || str == null)
//            return null;
//        else {
//            boolean rightIP = isRightIP(str);
//
//            if (rightIP) {
//                String location = iph.findGeography(str.toString());
//                String[] ipArray = location.split("\t");
//                if (beans == null || beans.length == 0) {
//                    return location;
//                } else if ("country".equals(beans[0]) && ipArray.length >= 1) {
//                    return ipArray[0];
//                } else if ("prov".equals(beans[0]) && ipArray.length >= 2) {
//                    return ipArray[1];
//                } else if ("city".equals(beans[0]) && ipArray.length >= 3) {
//                    return ipArray[2];
//                } else {
//                    return location;
//                }
//
//            } else {
//                return null;
//            }
//        }
//    }
//
//    static {
//        String uri = "hdfs://feng-cluster/udf/ip_id/ip/ip.dat";
//        iph.setPath(uri);
//        iph.initialize();
//    }
//
//    public static boolean matchAndPrint(String regex, String sourceText) {
//        Pattern pattern = Pattern.compile(regex);
//        Matcher matcher = pattern.matcher(sourceText);
//        while (matcher.find()) {
//            return true;
//        }
//        return false;
//    }
//
//    public static boolean isRightIP(String str) {
//        String regex = "^(((\\d{1,2})|(1\\d{2})|(2[0-4]\\d)|(25[0-5]))\\.){3}((\\d{1,2})|(1\\d{2})|(2[0-4]\\d)|(25[0-5]))$";
//        return matchAndPrint(regex, str);
//    }
//
//}
