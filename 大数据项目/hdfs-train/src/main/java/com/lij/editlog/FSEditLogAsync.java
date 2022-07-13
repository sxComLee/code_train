package com.lij.editlog;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Hadoop 3.x 新增异步日志写机制
 *  背后的具体工作情况：
 *  1、必定是大量线程往 editPendingQ 队列中，添加日志（ 50 个生产线程）
 *  2、有一个唯一的线程在消费队列，做 flush  （唯一的 flush 线程）
 *  有一个必须要注意的事项：
 *  txid 的 递增 和  日志的入队 必须在一把锁里面。
 *  这 50 个线程的哪个线程获取到 锁，就执行 txid++ 并且构造日志 入队
 *  -
 *  如果这一个 flush 线程的 flush 速度赶不上 100 个生产线程 生产日志入队的速度。
 *  改进方案：
 *  由当前这个 FSEditLogAsync 去封装批次 数据，构造成一个任务，提交到线程池
 *  1、如果不需要控制顺序，这就是很好的方案
 *  2、如果控制顺序，那么也是需要让这些 工作线程 控制顺序的
 */
public class FSEditLogAsync implements Runnable {

    // TODO_MA 马中华 注释： 用来存储日志的队列
    // TODO_MA 马中华 注释： 在 HDFS 的默认实现中， 这个队列的长度默认是 4096
    private final BlockingQueue<EditLog> editPendingQ = new ArrayBlockingQueue<EditLog>(100000);

    // TODO_MA 马中华 注释： 设计改良： 在 syncThread 线程的 run() 方法中，每隔一段时间做一次判断
    // TODO_MA 马中华 注释： 判断 队列中的日志的个数超过 100 就执行 flush

    // TODO_MA 马中华 注释： 用来消费上述队列的一个工作线程
    private Thread syncThread;

    private Long txid = 0L;

    public static void main(String[] args) {

        // TODO_MA 马中华 注释： 创建实例
        FSEditLogAsync fsEditLogAsync = new FSEditLogAsync();
        fsEditLogAsync.startThread();

        for (int i = 0; i < 50; i++) {
            new Thread(new Runnable() {
                public void run() {
                    for (int j = 0; j < 1000; j++) {
                        fsEditLogAsync.enqueueEdit("日志信息");
                    }

                }
            }).start();
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 入队的方法
     */
    private synchronized void enqueueEdit(String editContent) {
        EditLog log = new EditLog(getTxid(), editContent);
        if (!editPendingQ.offer(log)) {
            try {
                do {
                    this.wait(10); // will be notified by next logSync.
                } while (!editPendingQ.offer(log));
//                editPendingQ.offer(log);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private long getTxid() {
        return txid++;
    }

    @Override
    public void run() {
        while (true) {

            // TODO_MA 马中华 注释： 如果不是一日志一flush ，那么就加条件判断
            // TODO_MA 马中华 注释： 在这儿加：比如每 50 条做一次 flush

            // TODO_MA 马中华 注释： 尝试从 队列中，获取 日志
            EditLog edit = null;
            try {
                edit = dequeueEdit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // TODO_MA 马中华 注释： 如果获取到了则进行 flush
            if (edit != null) {
                logEdit(edit);
            }
        }
    }

    public void logEdit(EditLog edit) {
        System.out.println(edit.toString());
    }

    private EditLog dequeueEdit() throws InterruptedException {
        return editPendingQ.take();
    }

    private void startThread() {
        syncThread = new Thread(this, this.getClass().getSimpleName());
        syncThread.start();
    }

    class EditLog {

        long txid;
        String content;

        public EditLog(long txid, String content) {
            this.txid = txid;
            this.content = content;
        }

        @Override
        public String toString() {
            return "EditLog{" + "txid=" + txid + ", content='" + content + '\'' + '}';
        }
    }
}
