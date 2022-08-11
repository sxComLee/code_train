package current.newThread;

/**
 * Description:
 *  使用Thread 类的方法来创建线程类时，多个线程之间无法共享线程类的实例变量
 * @author lij
 * @date 2022-07-14 20:23
 */
public class FirstThread extends Thread{
    private int i;
    @Override
    public void run() {
        for ( i = 0; i < 100; i++) {
            // getName 直接调用父类的方法，返回线程的名字
            System.out.println(getName()+"  "+ i);
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100 ; i++) {
            System.out.println("当前主线程的名字： "+Thread.currentThread().getName()+"  "+ i);

            if(i == 5){
                //第一个线程
                new FirstThread().run();
                // 第二个线程
                new FirstThread().run();
            }
        }
    }
}
