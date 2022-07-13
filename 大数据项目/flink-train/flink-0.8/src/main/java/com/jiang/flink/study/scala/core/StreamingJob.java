/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jiang.flink.study.scala.core;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Random;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment.createLocalEnvironment();
//		StreamExecutionEnvironment.createRemoteEnvironment();


        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */
        DataStreamSource<Tuple2<String, String>> edits = env.addSource(new DataSource());

        KeyedStream<Tuple2<String, String>, String> keyedEdits = edits.keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<Tuple2<String, String>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    //迭代状态的初始值
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    //每一条输入数据，和迭代数据如何迭代
                    @Override
                    public Tuple2<String, Long> add(Tuple2<String, String> value, Tuple2<String, Long> accumulator) {
                        accumulator.f0 = value.f0;
                        accumulator.f1 += 1;
                        return accumulator;
                    }

                    //多个分区的迭代数据如何合并
                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    //返回数据，对最终的迭代数据如何处理，并返回结果。
                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });

        result.map(new MapFunction<Tuple2<String, Long>, String>() {

            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                return value.toString();
            }
        }).addSink(new FlinkKafkaProducer<String>("localhost:9092","result",new SimpleStringSchema()));

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    static class DataSource extends RichParallelSourceFunction {

        @Override
        public void run(SourceContext ctx) throws Exception {
            Random random = new Random();
            ctx.collect(new Tuple2<String, String>("key" + random.nextInt(), "value" + random.nextInt()));
        }

        @Override
        public void cancel() {

        }
    }
}
