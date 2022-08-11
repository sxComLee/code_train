package current.newThread;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * Description:
 *      像是Runnable接口的增强版,Callable接口提供了一个call()方法可以作为线程执行体,但call()方法比run()方法功能更强大。
 *          call() 方法可以有返回值
 *          call() 方法可以生命抛出异常
 *
 * @author lij
 * @date 2022-07-15 09:54
 */
public class FirstCallable implements Callable<Integer> {

    /**
     *
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws Exception {
        return null;
    }

    public static void main(String[] args) {
        FirstCallable st = new FirstCallable();

        FutureTask<Integer> task =  new FutureTask<Integer>( (Callable<Integer>)() -> {
            int i =0;
            for (i = 0; i < 100; i++) {
                System.out.println(Thread.currentThread().getName() + " 的循环变量i的值 "+ i);
            }
            //call 方法可以有返回值
            return i;
        });

        for (int i = 0; i < 100; i++) {
            System.out.println(Thread.currentThread().getName() + " 的循环变量i的值："+i);
            if(i== 20){
                new Thread(task,"有返回值的线程").start();
                // 注释打开后，需要等子线程执行完成之后，主线程才回继续执行，会阻塞
                // try{
                //     System.out.println("子线程的返回值："+task.get());
                // }catch (Exception e){
                //     e.printStackTrace();
                // }
            }
        }

        // task.get()方法会造成阻塞，直至子线程都处理完成之后才会继续主线程执行
        try{
            System.out.println("子线程的返回值："+task.get());
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("12345678");

    }
}
