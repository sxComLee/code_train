package com.jiang.flink.program.recommandSystemDemo.top;

import com.jiang.flink.program.recommandSystemDemo.domain.TopProductEntity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @ClassName TOpNHotItems
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 21:07
 * @Version 1.0
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, TopProductEntity, List<String>>  {

    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    private ListState<TopProductEntity> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<TopProductEntity> itemsStateDesc = new ListStateDescriptor<>(
                "itemState-state",
                TopProductEntity.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(TopProductEntity value, Context ctx, Collector<List<String>> out) throws Exception {
        itemState.add(value);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<String>> out) throws Exception {
        List<TopProductEntity> allItems = new ArrayList<>();
        for (TopProductEntity item : itemState.get()) {
            allItems.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<TopProductEntity>() {
            @Override
            public int compare(TopProductEntity o1, TopProductEntity o2) {
                return (int) (o2.getActionTimes() - o1.getActionTimes());
            }
        });
        List<String> ret = new ArrayList<>();
//        allItems.forEach(i-> ret.add(String.valueOf(i.getProductId())));
        for (int i = 0; i < topSize; i++) {
            ret.add(String.valueOf(allItems.get(i).getProductId()));
        }
        out.collect(ret);
    }


}
