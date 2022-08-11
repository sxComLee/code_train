package current.control;

/**
 * Description:
 *
 * @author lij
 * @date 2022-07-15 16:30
 */
public class DaemonThread extends Thread{
    @Override
    public void run() {
        for (int i = 0; i < 1000; i++) {
            System.out.println(getName()+"  "+i);
        }
    }

    public static void main(String[] args) {
        DaemonThread daemonThread = new DaemonThread();
        daemonThread.setDaemon(true);
        //启动后台线程
        daemonThread.start();

        for (int i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread().getName()+"  "+i);
        }

        // 程序执行到此处，前台县城（main线程）结束，后台线程也随之结束
    }
}
