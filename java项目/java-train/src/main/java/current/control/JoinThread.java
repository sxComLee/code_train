package current.control;

/**
 * Description:
 *   通过join方式控制线程
 * @author lij
 * @date 2022-07-15 16:12
 */
public class JoinThread extends Thread{
    // 提供有参数的构造器，设置线程的名字
    public JoinThread(String name){
        super(name);
    }

    /**
     * 重写run 方法，定义线程执行体
     */
    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            System.out.println(getName()+" "+i);
        }
    }

    public static void main(String[] args) throws Exception{
        new JoinThread("新线程启动").start();
        for (int i = 0; i < 100; i++) {
            if(i == 20){
                JoinThread rt = new JoinThread("被join的线程");
                rt.start();
                // main线程调用了jt线程的join 方法，main线程必须等rt线程执行结束后才会向下执行
                rt.join();
            }
            System.out.println(Thread.currentThread().getName()+ "   "+i);
        }
    }
}
