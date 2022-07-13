package com.jiang.flink.study.common.parameter;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * @ClassName GetParameter
 * @Description TODO 上面介绍了几种参数传递的方式，在日常的使用中，可能不仅仅是使用其中一种，
 *                      或许是某些的组合，比如通过parametertool来传递hdfs的路径，再通过filecache来读取缓存。
 * @Author jiang.li
 * @Date 2019-12-18 10:11
 * @Version 1.0
 */
public class GetParameter {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        //创建DataSet
        DataSource<String> text = env.fromElements("123", "456");




        /**
         * ++++++++++++++++  动态的参数传递  ++++++++++++++++
         */
        //=================  通过 onnectStream 关联获取参数   ================
        //如果参数本身会随着时间发生变化，需要使用connectStream，即将流进行聚合
        //假设规则数据也是一个流，这个流通过restful服务进行发布

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("topic1",new SimpleStringSchema(),new Properties());
        DataStreamSource source1 = streamEnv.addSource(consumer);

        FlinkKafkaConsumer consumer1 = new FlinkKafkaConsumer("topic2",new SimpleStringSchema(),new Properties());
        DataStreamSource source2 = streamEnv.addSource(consumer1);

        ConnectedStreams connect1 = source1.connect(source2);
        connect1.flatMap(new RichCoFlatMapFunction() {
            @Override
            public void flatMap1(Object value, Collector out) throws Exception {

            }

            @Override
            public void flatMap2(Object value, Collector out) throws Exception {

            }
        });

        //=================  通过 从task发回到client 关联获取参数   ================
        //flink 内置的accumulator
        //IntCounter, LongCounter, DoubleCounter – allows summing together int, long, double values sent from task managers
        //AverageAccumulator – calculates an average of double values
        //LongMaximum, LongMinimum, IntMaximum, IntMinimum, DoubleMaximum, DoubleMinimum – accumulators to determine maximum and minimum values for different types
        //Histogram – used to computed distribution of values from task managers
        text.flatMap(new RichFlatMapFunction<String, String>() {
            private IntCounter linesNum = new IntCounter();
            Histogram histogram = new Histogram();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("lineNums", linesNum);
            }

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
                linesNum.add(1);
            }


        });

        //如果需要自定义accumulator，只需要实现Accumulator或者SimpleAccumulator接口即可

        int lineNums = env.getLastJobExecutionResult().getAccumulatorResult("lineNums");
        System.out.println(lineNums);


        /**
         * ++++++++++++++++  静态的参数传递  ++++++++++++++++
         */
        /*
         * *************  大量参数传递  ***************
         **/
        //=================  通过 distributedCache 获取参数   ================
        //在定义dag图的时候指定缓存文件,flink本身支持指定本地的缓存文件，但一般而言，建议指定分布式存储比如hdfs上的文件，并为其指定一个名称。
        env.registerCachedFile("hdfs:///user/to/file", "registerFileName" );
        text.filter(new RichFilterFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                File registerFileName = getRuntimeContext().getDistributedCache().getFile("registerFileName");
                //获取到缓存文件之后进行接下来的操作
            }

            @Override
            public boolean filter(String s) throws Exception {
                return false;
            }
        });


        //=================  通过 broadcast 对象获取参数   ================
        //广播的变量会保存在tm的内存中，这个也必然会使用tm有限的内存空间，也因此不能广播太大量的数据。
        DataSource<String> brocadcast = env.fromElements("123", "124");
        text.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).withBroadcastSet(brocadcast, "brocadcast");


        /*
         * *************  少量参数传递  ***************
         **/
        //=================通过 ParameterTool 对象获取参数，这个方式只能在DataSet，DataStream中都可以获取 ================
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
       text.filter(new RichFilterFunction<String>() {
           @Override
           public boolean filter(String value) throws Exception {
               String[] genres = value.split("\\|");
               // Get global parameters
               ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
               // Read parameter
               String genre = parameterTool.get("genre","");

               return Stream.of(genres).anyMatch(g -> g.equals(genre));
           }
       }).print();


        //================= 通过Configuration 对象获取参数，这个方式只能在DataSet中获取 ================
        //缺点不够动态，每次参数改变，都涉及到程序的变更
        Configuration conf = new Configuration();
        //设置对应的键值对
        conf.setString("genre", "Value");

        text
            .withParameters(conf)
                //这里的函数必须是Rich的，不然没办法传递参数
            .filter(new RichFilterFunction<String>() {
                String genre;

                @Override
                public void open(Configuration parameters) throws Exception {
                    // Read the parameter，如果没有的话，指定默认值
                    genre = parameters.getString("genre", "");
                }

                @Override
                public boolean filter(String movie) throws Exception {
                    String[] genres = movie.split("\\|");

                    return Stream.of(genres).anyMatch(g -> g.equals(genre));
                }
            }).print();


    }

}
