package udf;

import junit.framework.TestCase;

/**
 * Created by yqlong on 2018/3/8.
 */
public class UDFTransCodeWindFengjrTest extends TestCase {
    UDFTransCodeWindFengjr udfTransCode;
    public void setUp() {
        udfTransCode = new UDFTransCodeWindFengjr();
    }

    public void testEvaluate() throws Exception {

    }

    public void testWindIndexCodeToFengjr() throws Exception {
        String[][] testString = {{"m50102.SH", "m50102.SH"},
        {"101220.CIC", "101220.CIC"},
        {"399319.SZ", "399319.HSZS"},
        {"886048.WI", "886048.GNZS"},
        {"884123.WI", "884123.GNZS"},
        {"930806.CSI", "930806.CSI"},
        {"h30217.CSI", "h30217.CSI"},
        {"801124.SI", "801124.SI"},
        {"399978.SZ", "399978.HSZS"},
        {"h30255.CSI", "h30255.CSI"},
        {"950096.CSI", "950096.CSI"},
        {"h30186.CSI", "h30186.CSI"},
        {"930745.CSI", "930745.CSI"},
        {"950079.CSI", "950079.CSI"},
        {"399136.SZ", "399136.HSZS"},
        {"801164.SI", "801164.SI"},
        {"100420.CIC", "100420.CIC"},
        {"850553.SI", "850553.SI"},
        {"399001.SZ", "399001.HSZS"},
        {"801182.SI", "801182.SI"},
        {"399243.SZ", "399243.HSZS"},
        {"850343.SI", "850343.SI"},
        {"399663.SZ","399663.HSZS"},
        {"h20750.CSI", "h20750.CSI"},
        {"399976.CSI", "399976.CSI"},
        {"n11072.CSI", "n11072.CSI"},
        {"CI005394.WI", "005394.HYZS"},
        {"n11012.CSI", "n11012.CSI"},
        {"h00971.CSI", "h00971.CSI"},
        {"h50004.SH", "h50004.SH"},
        {"100920.CIC", "100920.CIC"},
        {"000826.SH", "000826.HSZS"},
        {"s00802.SZ", "s00802.SZ"},
        {"801751.SI", "801751.SI"},
        {"h11154.CSI", "h11154.CSI"},
        {"CI002043.WI", "002043.HYZS"},
        {"399108.SZ", "399108.HSZS"},
        {"884081.WI", "884081.GNZS"},
        {"930608.CSI", "930608.CSI"},
        {"h11086.SH", "h11086.SH"},
        {"399703.SZ", "399703.HSZS"},
        {"FEFI.FMM", "FEFI.FMM"},
        {"h01108.CSI", "h01108.CSI"},
        {"h40101.SH", "h40101.SH"},
        {"n30826.CSI", "n30826.CSI"}};

        for (String[] code: testString) {
            assertEquals(code[1], udfTransCode.windIndexCodeToFengjr(code[0]));
        }
    }
}