package udf;

import junit.framework.Assert;
import junit.framework.TestCase;

import java.util.ArrayList;

/**
 * Created by ran.li on 2019/12/01.
 */
public class UDFJsonArrayTest extends TestCase {

    public void testEvaluate() throws Exception {

        UDFJsonArray udfJsonArray = new UDFJsonArray();

        String json = "[{\"name\":\"jack\"},{\"name\":\"mark\"},{\"name\":\"bill\"},{\"name\":\"john\"}]";

        Object[] objects = new Object[3];
        objects[0] = json;
        objects[1] = "2019-11-12";
        objects[2] = "INSERT";


        ArrayList<String[]> results = udfJsonArray.processInputRecord(objects);
        System.out.println(results.size());
        //Assert.assertEquals(4, results.size());
        Assert.assertEquals("{\"name\":\"jack\"}", results.get(0)[0].toString());
//        Assert.assertEquals("2019-11-12", results.get(0)[1]);
//        Assert.assertEquals("INSERT", results.get(0)[2]);

        System.out.println(results.get(0)[0]);
//        System.out.println(results.get(0)[1]);
//        System.out.println(results.get(1)[0]);
//        System.out.println(results.get(2)[0]);
////        System.out.println(results.get(0)[1]);
////        System.out.println(results.get(0)[2]);
//
//        Object[] objects2 = new Object[1];
//        objects2[0] = json;
//
//        ArrayList<String[]> results2 = udfJsonArray.processInputRecord(objects2);
//        System.out.println(results2.get(0)[0]);

//        System.out.println(results.get(0)[3]);



    }

}