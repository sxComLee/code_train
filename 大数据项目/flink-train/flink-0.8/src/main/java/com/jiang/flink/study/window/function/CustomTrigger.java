package com.jiang.flink.study.window.function;

import com.jiang.flink.study.common.model.WordEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName CustomTrigger
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-29 16:37
 * @Version 1.0
 */
@Slf4j
public class CustomTrigger extends Trigger<WordEvent, TimeWindow> {
    ReducingState<Long> stateDesc;

    private long interval;

    private CustomTrigger(long interval){this.interval = interval;}

    public CustomTrigger() {
        super();
    }

    //每个元素在被添加到窗口时调用这个方法
    @Override
    public TriggerResult onElement(WordEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        log.info("======onElement====window start = {}, window end = {}", window.getStart(), window.getEnd());
        //什么都不做
        return TriggerResult.CONTINUE;
    }


    //当一个已注册的 ProcessingTime 计时器启动时调用
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        log.info("======onProcessingTime====");
        return null;
    }

    //当一个已注册的 EventTime 计时器启动时调用
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        log.info("======onEventTime====");
        return null;
    }

    //与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态
    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        super.onMerge(window, ctx);
    }

    //执行任何需要清除的相应窗口
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }

    public static CustomTrigger create(){return new CustomTrigger();}

}
