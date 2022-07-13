package com.lij.editlog;

import java.util.LinkedList;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 高并发记录日志双写缓冲方案设计实现
 *  1、高并发
 *  2、数据一致性+安全
 *  内部的核心思想： 双写缓冲  +  分段加锁
 */
public class FSEditLog {

    public static void main(String[] args) {
        final FSEditLog fsEditLog = new FSEditLog();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动一堆线程来完成高并发测试
         */
        for (int i = 0; i < 50; i++) {
            new Thread(new Runnable() {
                public void run() {
                    for (int j = 0; j < 1000; j++) {
                        fsEditLog.logEdit("日志信息");
                    }

                }
            }).start();
        }
        // TODO_MA 马中华 注释： 这 50 个线程中，同时只有一个线程在做 flush ，但是到底是哪一个线程我不知道
    }

    // TODO_MA 马中华 注释： 每一条日志，都有一个全局的递增 ID
    // TODO_MA 马中华 注释： txid++ 这句代码会加锁
    private long txid = 0L;

    // TODO_MA 马中华 注释： 双写缓冲
    private DoubleBuffer editLogBuffer = new DoubleBuffer();

    //当前是否正在往磁盘里面刷写数据
    private volatile Boolean isSyncRunning = false;
    private volatile Boolean isWaitSync = false;

    // TODO_MA 马中华 注释： 完成了 flush 的最大的 txid
    private volatile Long syncMaxTxid = 0L;

    // TODO_MA 马中华 注释： 线程副本，保存txid
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 记录一条日志
     *  作用： 把日志写到双写缓冲
     */
    public void logEdit(String content) {
        //加了一把锁
        synchronized (this) {
            txid++;
            localTxid.set(txid);
            EditLog log = new EditLog(txid, content);
            editLogBuffer.write(log);
        } //释放锁

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 同步 / flush
         *  具体情况，在 HDFS 中，一定会做一个判断，如果满足条件，才做 flush
         *  1、先交换 两个 缓冲
         *  2、做 syncBuffer 的 flush
         */
        logSync();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： flush
     */
    private void logSync() {

        // TODO_MA 马中华 注释： 第一段加锁
        synchronized (this) {
            // TODO_MA 马中华 注释： 如果 isSyncRunning = true，表示有其他线程正在执行 flush
            if (isSyncRunning) {
                long txid = localTxid.get();
                if (txid <= syncMaxTxid) {
                    return;
                }
                if (isWaitSync) {
                    return;
                }
                isWaitSync = true;
                while (isSyncRunning) {
                    try {
                        wait(2000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                isWaitSync = false;
            }

            // TODO_MA 马中华 注释： 做交换
            editLogBuffer.setReadyToSync();

            if (editLogBuffer.syncBuffer.size() > 0) {
                syncMaxTxid = editLogBuffer.getSyncMaxTxid();
            }
            //
            isSyncRunning = true;

        } //线程一 释放锁

        // TODO_MA 马中华 注释： flush 的真正执行，代码效率低
        editLogBuffer.flush();

        //重新加锁
        // TODO_MA 马中华 注释： 第二段加锁
        synchronized (this) {
            //线程一 赋值为false
            isSyncRunning = false;
            //唤醒等待线程。
            notify();
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 日志抽象，一个对象代表一条日志
     */
    class EditLog {

        long txid;

        // TODO_MA 马中华 注释： 在 HDFS 的实现中，MkdirOp 这是一条日志
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

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 双缓冲方案
     */
    class DoubleBuffer {

        // TODO_MA 马中华 注释： 在 HDFS 源码实现中，是一个 Buffer
        LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();
        LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

        public void write(EditLog log) {
            currentBuffer.add(log);
        }

        // TODO_MA 马中华 注释： HDFS 的方法名就叫这个
        public void setReadyToSync() {
            LinkedList<EditLog> tmp = currentBuffer;
            currentBuffer = syncBuffer;
            syncBuffer = tmp;
        }

        public Long getSyncMaxTxid() {
            return syncBuffer.getLast().txid;
        }

        public void flush() {
            for (EditLog log : syncBuffer) {
                // TODO_MA 马中华 注释： 这是我的模拟实现
                System.out.println("存入磁盘日志信息：" + log);
                // TODO_MA 马中华 注释： 对于 HDFS 源码来说，其实是通过 输出流写入 日数据到磁盘文件
            }
            syncBuffer.clear();
        }
    }
}
