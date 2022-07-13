package com.jiang.flink.study.window.function;

import com.jiang.flink.study.common.model.WordEvent;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @ClassName CustomSource 自己构造一个数据缘
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-29 16:33
 * @Version 1.0
 */
public class CustomSource extends RichSourceFunction<WordEvent> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<WordEvent> ctx) throws Exception {
        while (isRunning){
            ctx.collect(new WordEvent(word(),count(),System.currentTimeMillis()));
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
    private String word(){
        String[] strs = new String[]{"a","b","c","d"};
        int index = (int)(Math.random() % strs.length);
        return strs[index];
    }

    private int count() {
        int[] strs = new int[]{1, 2, 3, 4, 5, 6};
        int index = (int) (Math.random() % strs.length);
        return strs[index];
    }

}
