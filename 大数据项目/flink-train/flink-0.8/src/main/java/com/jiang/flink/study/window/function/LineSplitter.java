package com.jiang.flink.study.window.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName LineSplitter
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-29 16:46
 * @Version 1.0
 */
@Slf4j
public class LineSplitter implements FlatMapFunction<String, Tuple2<Long,String>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<Long, String>> out) throws Exception {
        String[] tokens = value.split(" ");
        if(tokens.length>2 && isValidLong(tokens[0])){
            out.collect(new Tuple2<>(Long.parseLong(tokens[0]),tokens[1]));
        }
    }

    private static boolean isValidLong(String str) {
        try {
            long _v = Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            log.info("the str = {} is not a number", str);
            return false;
        }
    }
}
