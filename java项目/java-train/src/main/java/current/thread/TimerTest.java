package current.thread;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @ClassName TimerTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-27 10:20
 * @Version 1.0
 */
public class TimerTest {
    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new MyTask(), 1,200);
    }
}

class MyTask extends TimerTask{

    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println("hello come on");
        }
    }
}
