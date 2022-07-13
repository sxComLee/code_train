package com.mazh.nx.hdfs3.synchronized_lock;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： synchronized 加锁就是可重入的
 */
public class SynchronizedLockTest {

    public static void main(String[] args) throws InterruptedException {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (SynchronizedLockTest.class){
                    System.out.println("4");
                }
            }
        }).start();

        // TODO_MA 马中华 注释： 第一次获取锁
        synchronized (SynchronizedLockTest.class){
            System.out.println("1");
            Thread.sleep(2000);

            // TODO_MA 马中华 注释： 第二次获取锁
            synchronized (SynchronizedLockTest.class){
                System.out.println("2");
                Thread.sleep(2000);
            }
            System.out.println("3");
        }
    }
}
