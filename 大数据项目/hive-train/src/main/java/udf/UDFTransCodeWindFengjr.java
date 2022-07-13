package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by yqlong on 2018/3/8.
 */
public class UDFTransCodeWindFengjr extends UDF {

    /**
     * 万得代码和凤金代码相互转换
     * @param code 资产代码，后缀：股票HSGP、沪深综合指数HSZS、行业指数HYZS、概念指数GNZS、场内基金CNJJ
     * @param type int，转换类型，0 凤金代码=> 万得代码，1：万得代码=>凤金代码
     * @return
     */
    public String evaluate(String code, int type){
        if (1 == type){
            return windIndexCodeToFengjr(code);
        }
        return code;
    }

    public String windIndexCodeToFengjr(String code) {
        if (code == null || code.trim().length() == 0) {
            return "";
        } else if ((code.startsWith("60") && code.endsWith("SH"))
                || ((code.startsWith("00") || code.startsWith("30")) && code.endsWith(("SZ")))) {
            return code.replace("SH", "HSGP").replace("SZ", "HSGP");
        } else if ((code.startsWith("000") && code.endsWith("SH"))
                || (code.startsWith("399") && code.endsWith("SZ"))) {
            return code.replace("SZ", "HSZS").replace("SH", "HSZS");
        } else if (code.startsWith("CI") && code.endsWith("WI")) {
            return code.substring(2, code.length() - 2) + "HYZS";
        } else if (code.startsWith("88") && code.endsWith("WI")) {
            return code.substring(0, code.length() - 2) + "GNZS";
        } else if (code.length() == 8 && (code.startsWith("M") || code.startsWith("S") || code.startsWith("G"))) {
            return code + ".HGZS";
        } else {
            return code;
        }

    }
}
