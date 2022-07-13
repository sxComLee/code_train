package com.jiang.flink.study.tableApi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.List;


/**
 * @ClassName WordCount
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-10-31 10:42
 * @Version 1.0
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bEnv = BatchTableEnvironment.create(env);

        /**
         * ——————————————————————————————————
         * 关于注册一个Table
         * ——————————————————————————————————
         **/

        //获取文件地址
        String path = WordCount.class.getClassLoader().getResource("test/words.txt").getPath();
        ConnectTableDescriptor connectTableDescriptor = bEnv.connect(new FileSystem().path(path))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING));

        /**通过 Table descriptor 注册一个表名叫做fileSource的表**/
        connectTableDescriptor.registerTableSource("fileSource");


        //用户自定义注册Table source
        CsvTableSource csvTableSource = new CsvTableSource(path, new String[]{"word"}, new TypeInformation[]{Types.STRING});
        /**通过用户自定义source 注册一个表名为 table 的表**/
        bEnv.registerTableSource("table",csvTableSource);


        //通过DataStream注册
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment sEnv = StreamTableEnvironment.create(streamEnv);
        //设定时间类型 为eventTime
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> stream = streamEnv.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                ctx.collect("");
            }

            @Override
            public void cancel() {

            }
        });
        /**通过DataDstream 注册一个 表名为 table1 的表**/
        sEnv.registerDataStream("table1",stream);
        //指定processingtime。对应的字段必须是在最后
        sEnv.fromDataStream(stream,"f2,f1.proctime");
        //指定Event time。对应的字段可以不在最后
        sEnv.fromDataStream(stream,"f1.rowtime,f2");

        /**
         * ——————————————————————————————————
         * 关于处理一个Table
         * ——————————————————————————————————
         **/
        Table result = bEnv.scan("fileSource")
                .groupBy("word")
                .select("word, count(1) as count");

        /** 提升易用性的操作 **/
        result.addColumns("concat(c,'sunny') as desc");
        result.addOrReplaceColumns("concat(c,'sunny') as desc");
        result.dropColumns("b,c");
        result.renameColumns("b as b2,c as c2");
        //可以是字段名称的连续，可以是从一个columnIndex到另一个columnIndex，可以是从一个字段名称到另一个字段名称
        result.select("withColumns( 2 to 4 ,a,b, firstFileddName to thiredFiledName)");
        result.select("withOutColumns( 2 to 4)");

        /** 增强功能的操作 **/


        DataSet<Row> rowDataSet = bEnv.toDataSet(result, Row.class);

        rowDataSet.print();


        /**
         * ——————————————————————————————————
         * 关于输出一个Table
         * ——————————————————————————————————
         **/

        /** 通过 Table descriptor 输出 **/
        connectTableDescriptor.registerTableSink("fileSink");

        /** 通过 Table descriptor 输出 **/
        CsvTableSink csvTableSink = new CsvTableSink(path, "\t", 1, org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE);
        bEnv.registerTableSink("table",csvTableSink);

        /** 通过 DataStream 输出 **/
        sEnv.toRetractStream(result,Row.class);

    }

    /**
     * @Author jiang.li
     * @Description //TODO 通过TableSource生成processing time
     * @Date 14:11 2019-11-01
     * @Param
     * @return
     **/
    class MytableSource extends CsvTableSource  implements DefinedProctimeAttribute{

        public MytableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            super(path, fieldNames, fieldTypes);
        }

        public MytableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes, String fieldDelim, String lineDelim, Character quoteCharacter, boolean ignoreFirstLine, String ignoreComments, boolean lenient) {
            super(path, fieldNames, fieldTypes, fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
        }

        public MytableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes, int[] selectedFields, String fieldDelim, String lineDelim, Character quoteCharacter, boolean ignoreFirstLine, String ignoreComments, boolean lenient) {
            super(path, fieldNames, fieldTypes, selectedFields, fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
        }

        @Nullable
        @Override
        public String getProctimeAttribute() {
            return null;
        }

        @Override
        public DataType getProducedDataType() {
            return null;
        }
    }

    /**
     * @Author jiang.li
     * @Description //TODO 通过TableSource生成Event time
     * @Date 14:12 2019-11-01
     * @Param
     * @return
     **/
    class MyEventTableSource extends CsvTableSource implements DefinedRowtimeAttributes{

        public MyEventTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            super(path, fieldNames, fieldTypes);
        }

        public MyEventTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes, String fieldDelim, String lineDelim, Character quoteCharacter, boolean ignoreFirstLine, String ignoreComments, boolean lenient) {
            super(path, fieldNames, fieldTypes, fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
        }

        public MyEventTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes, int[] selectedFields, String fieldDelim, String lineDelim, Character quoteCharacter, boolean ignoreFirstLine, String ignoreComments, boolean lenient) {
            super(path, fieldNames, fieldTypes, selectedFields, fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
        }

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            return null;
        }

        @Override
        public DataType getProducedDataType() {
            return null;
        }
    }


}
