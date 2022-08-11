package current.newThread;

/**
 * @ClassName SingleThreadTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-26 13:32
 * @Version 1.0
 */
public class RunnableTest {
    public static void main(String[] args) {
        Racer racer = new Racer();
        Thread t1 = new Thread(racer, "turtle");
        Thread t2 = new Thread(racer, "rabbit");
        t1.start();
        t2.start();
    }

}


class Racer implements Runnable{
    private static String winner;

    public void run() {
        for (int steps = 1; steps <= 100; steps++) {
            if(Thread.currentThread().equals("rabbit") && steps%10 ==0){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(Thread.currentThread().getName()+"->"+steps);

        }
    }
}
