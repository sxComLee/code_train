package com.jiang.flink.study.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @ClassName HbaseSink
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-13 10:23
 * @Version 1.0
 */
public class HbaseSink extends RichSinkFunction<String> {
    private Connection conn;
    String tableName = "";
    String cloumnFamily = "";

    public HbaseSink(String tableName, String cloumnFamily) {
        this.tableName = tableName;
        this.cloumnFamily = cloumnFamily;
    }

    /**
     * @return void
     * @Author jiang.li
     * @Description //TODO 创建链接的方法
     * @Date 10:59 2019-11-13
     * @Param [parameters]
     **/
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        //获取当前文件路径
        String currDir = System.getenv().get("PWD");
        File keytabFile = new File(currDir + "/keytab", "test/hbase.keytab");
        URL resources = HbaseSink.class.getClassLoader().getResource("hbase.keytab");
//        System.out.println(resources);
        String resource = resources.toString();
        String keytabPath = keytabFile.getAbsolutePath();

        UserGroupInformation.setConfiguration(conf);
        if(UserGroupInformation.isSecurityEnabled()){
            UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase@hadoop_edw", resource);
        }

        conf.set(HConstants.ZOOKEEPER_QUORUM,"fdw6.fengjr.inc,fdw4.fengjr.inc,fdw5.fengjr.inc,fjr-yz-204-11,fjr-yz-204-13");
        conf.set("zookeeper.znode.parent","/hbase_test");
//        conf.set(HConstants.ZOOKEEPER_QUORUM,"fjr-off-51-25:2181,fjr-ofckv-73-118:2181,slave02:2181");


        conn = ConnectionFactory.createConnection(conf);

    }

    /**
     * @return void
     * @Author jiang.li
     * @Description //TODO String 是一个JsonArray，由一个个的jsonObject组成，其中key为rowkey，value为jsonObject（key为属性名，value为属性值）
     * @Date 11:31 2019-11-13
     * @Param [value, context]
     **/
    @Override
    public void invoke(String value, Context context) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        JSONArray jsonArray = JSON.parseArray(value);
        Iterator<Object> iterator = jsonArray.iterator();
        ArrayList<Put> puts = new ArrayList<>();
        while (iterator.hasNext()) {
            //内层的json，key为rowkey，value为jsonObject（key为属性名，value为属性值）组成的jsonArray，
            JSONObject jsonObject = JSON.parseObject(iterator.next().toString());
            for (String rowkey : jsonObject.keySet()) {
                Put put = new Put(Bytes.toBytes(rowkey));
                JSONObject fieldAndValue = JSON.parseObject(jsonObject.get(rowkey).toString());
                for (String field : fieldAndValue.keySet()) {
                    //将每一个属性值都插入到hbase中
                    put.addColumn(Bytes.toBytes(cloumnFamily), Bytes.toBytes(field), Bytes.toBytes(fieldAndValue.get(field).toString()));
                }
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
    }


    @Override
    public void close() throws Exception {
        super.close();
        conn.close();
    }
}
