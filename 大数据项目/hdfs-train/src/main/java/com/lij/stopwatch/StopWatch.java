package com.lij.stopwatch;

import java.util.concurrent.TimeUnit;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： HDFS 中的秒表 模拟实现
 */
public class StopWatch {

    // TODO_MA 马中华 注释： 秒表启动状态，为 true 的时候表示启动了
    private boolean isStarted;

    // TODO_MA 马中华 注释： 启动时间
    private long startMillis;

    // TODO_MA 马中华 注释： 流逝时间(秒表的计时时间)
    private long currentElapsedMillis;

    public StopWatch() {
    }

    public static void main(String[] args) throws InterruptedException {

        // TODO_MA 马中华 注释： 构造一个秒表对象
        StopWatch stopWatch = new StopWatch();

        // TODO_MA 马中华 注释： 秒表启动
        stopWatch.start();

        // TODO_MA 马中华 注释： 模拟业务执行了 1s
        Thread.sleep(2000);

        // TODO_MA 马中华 注释： 秒表停止
        stopWatch.stop();

        // TODO_MA 马中华 注释： 再模拟1s， 为了演示，这个 1s 对秒表的计时是没有影响的
        Thread.sleep(1000);
        long elapsedNanos = stopWatch.getElapsedMillis();
        System.out.println(elapsedNanos);
    }

    public StopWatch start() {
        if (isStarted) {
            System.out.println("秒表已经启动");
            return this;
        }
        isStarted = true;
        // TODO_MA 马中华 注释： 记录启动时间
        startMillis = System.currentTimeMillis();
        return this;
    }

    public StopWatch stop() {
        if (!isStarted) {
            System.out.println("秒表已经停止");
            return this;
        }
        long now = System.currentTimeMillis();

        // TODO_MA 马中华 注释： 得到秒表的计时时间
        currentElapsedMillis += now - startMillis;
        isStarted = false;
        return this;
    }

    public StopWatch reset() {
        currentElapsedMillis = 0;
        isStarted = false;
        return this;
    }

    public long getElapsedMillis() {
        return isStarted ? System.currentTimeMillis() - startMillis + currentElapsedMillis : currentElapsedMillis;
    }

    public void close() {
        if (isStarted) {
            stop();
        }
    }
}
