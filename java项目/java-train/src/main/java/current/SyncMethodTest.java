package current;

/**
 * @ClassName SyncMethodTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-26 16:22
 * @Version 1.0
 */
public class SyncMethodTest {
    public static void main(String[] args) {

        Person person = new Person();
        new Thread(new Person(), "dou").start();
        new Thread(new Person(), "fang").start();
    }


}

class Person implements Runnable{
    @Override
    public void run() {
        nothing();
    }


    public synchronized void nothing(){
        for (int i = 0; i < 10 ; i++) {
            System.out.println(Thread.currentThread().getName());
        }

    }
}

class Person1 implements Runnable{
    String name = "love";
    @Override
    public void run() {
        nothing();
    }


    public synchronized void nothing(){
        for (int i = 0; i < 100 ; i++) {
            System.out.println(Thread.currentThread().getName()+"->"+name);
        }

    }
}
