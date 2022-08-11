package current.newThread;

/**
 * Description:
 *  Runnable 对象仅作为Thread对象的Target，实现类里包含的run()作为线程执行体
 *      实际的线程对象依然是Thread 实例，只是改Thread线程负责执行其Target的run()方法
 *
 *      采用Runnable接口的方式创建的多个线程可以共享线程类的实例变量
 * @author lij
 * @date 2022-07-14 20:34
 */
public class FirstRunnable implements Runnable{
    private int i ;
    @Override
    public void run() {
        for (i = 0; i < 10 ; i++) {
            System.out.println(Thread.currentThread().getName()+"   "+i);
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            System.out.println(Thread.currentThread().getName()+"      "+i);

            if(i == 20){
                FirstRunnable st = new FirstRunnable();
                // 第一个线程
                new Thread(st,"第一个线程").start();
                //第二个线程
                new Thread(st,"第二个线程").start();
            }
        }
    }
}
