package udf;//package udf;
//
///**
// * Created by ran.li on 2019-11-13
// */
//
//import com.fengjr.edw.utils.AESUtils;
//import org.apache.hadoop.hive.ql.exec.UDF;
//
//public class UDFSaltEncrypt extends UDF {
//
//    public String evaluate(String str,String salt) {
//
//        try {
//
//            return AESUtils.Encrypt(str, salt);
//
//        } catch (Exception e) {
//
//            return null;
//
//        }
//
//    }
//
//}