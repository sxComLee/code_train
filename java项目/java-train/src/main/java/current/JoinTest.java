package current;

/**
 * @ClassName JoinTest
 * @Description TODO 插队线程
 * @Author jiang.li
 * @Date 2019-12-26 13:55
 * @Version 1.0
 */
public class JoinTest {
    public static void main(String[] args) {
        System.out.println("一个做饭的例子");

        Thread cook = new Thread(new CookSTH(), "cook");
        cook.start();

    }
}

class  CookSTH implements Runnable {
    @Override
    public void run() {
        System.out.println("准备开始做饭，发现厨房未清理");
        Thread clean = new Thread(new cleanKitchen(), "cook");
        clean.start();
        try {
            clean.join();
            System.out.println("厨房清理了，开始做饭");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


class cleanKitchen implements Runnable{

    @Override
    public void run() {
        System.out.println("开始进行厨房清洁");
    }
}