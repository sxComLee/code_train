package current.control;

/**
 * Description:
 *
 * @author lij
 * @date 2022-07-15 16:37
 */
public class SleepThread {

    public static void main(String[] args) throws Exception{
        for (int i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread().getName()+"  "+i);
            // 调用sleep方法让当前线程暂停1s
            Thread.sleep(1000);
        }
    }
}
