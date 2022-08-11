package current.control;

/**
 * Description:
 *
 * @author lij
 * @date 2022-07-15 19:39
 */
public class PriorityThread extends Thread{
    public PriorityThread(String name){
        super(name);
    }

    @Override
    public void run() {
        for (int i = 0; i < 50; i++) {
            System.out.println("当前线程的为："+getName()+"  当前线程优先级为："+getPriority()+" 序号为："+i);
        }
    }

    public static void main(String[] args) {
        // 设置主线程的优先级为 6
        Thread.currentThread().setPriority(6);
        for (int i = 0; i < 30; i++) {
            if(i == 10){
                PriorityThread low = new PriorityThread("低优先级线程");
                System.out.println(low.getName() +" 创建之初的优先级为："+ low.getPriority() );
                low.setPriority(PriorityThread.MIN_PRIORITY);
                System.out.println(low.getName() +" 修改之后的优先级为："+ low.getPriority());
                //低优先级线程启动
                low.start();
            }

            if(i == 20){
                PriorityThread high = new PriorityThread("高优先级线程");
                System.out.println(high.getName() +" 创建之初的优先级为："+ high.getPriority());
                high.setPriority(PriorityThread.MAX_PRIORITY);
                System.out.println(high.getName() +" 修改之后的优先级为："+ high.getPriority());
                //低优先级线程启动
                high.start();
            }
        }
    }
}
