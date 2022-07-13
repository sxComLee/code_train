package com.jiang.flink.study.scala.core.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @ClassName AsyncdataSource
 * @Description TODO 异步IO访问外部数据源
 * @Author jiang.li
 * @Date 2019-11-15 13:29
 * @Version 1.0
 */
public class AsyncdataSource extends RichAsyncFunction<Integer,String> {

    @Override
    public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) {
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return "test";
            }
        }).thenAccept( (String result) -> {
            //            resultFuture.completeExceptionally(new Exception("wawawa"));
//            resultFuture.complete(Collections.singleton(input.intValue() + "," + result));
        });
    }
    //等待超时时间
    @Override
    public void timeout(Integer input, ResultFuture<String> resultFuture) {
        super.timeout(input, resultFuture);
    }

    //准备工作
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    //收尾工作
    @Override
    public void close() throws Exception {
        super.close();
    }

}
