package com.fengjr.sparkproject.spark;

import com.fengjr.sparkproject.conf.ConfigurationManager;
import com.fengjr.sparkproject.constant.Constants;
import com.fengjr.sparkproject.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @ClassName UserVisitSessionAnalyzeSpark
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-10-17 17:48
 * @Version 1.0
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        //构建spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_LOCAL_TASKID_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());
        //生成模拟数据
        mockData(sc,sqlContext);
        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    private static void mockData(JavaSparkContext sc,SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc,sqlContext);
        }
    }
}
