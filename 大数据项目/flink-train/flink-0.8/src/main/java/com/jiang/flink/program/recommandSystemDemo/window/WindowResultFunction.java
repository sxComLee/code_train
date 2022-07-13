package com.jiang.flink.program.recommandSystemDemo.window;

import com.jiang.flink.program.recommandSystemDemo.domain.TopProductEntity;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName WindowResultFunction
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 21:04
 * @Version 1.0
 */
public class WindowResultFunction
        implements WindowFunction<Long, TopProductEntity, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> aggregateResult, Collector<TopProductEntity> collector) throws Exception {
        int itemId = key.getField(0);
        Long count = aggregateResult.iterator().next();
        collector.collect(TopProductEntity.of(itemId,window.getEnd(),count));
    }
}
