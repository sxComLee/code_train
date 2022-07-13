package current;

import lombok.AllArgsConstructor;

/**
 * @ClassName deadLockDemo
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-26 15:21
 * @Version 1.0
 */
public class deadLockDemo {
    public static void main(String[] args) {
        new MakeUp(false, "豆豆").start();
        new MakeUp(true, "芳").start();
    }
}

@AllArgsConstructor
class MakeUp extends Thread{
    boolean flag;
    String name;
    static Lipstick lip = new Lipstick();
    static Mirror mir = new Mirror();
    @Override
    public void run() {
        if(flag){
            synchronized (lip){
                System.out.println(name + " 拿起了口红");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                synchronized (mir){
//                    System.out.println(name + " 拿起了镜子");
//                }
            }
            synchronized (mir){
                System.out.println(name + " 拿起了镜子");
            }
        }else{
            synchronized (mir){
                System.out.println(name + " 拿起了镜子");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                synchronized (lip){
//                    System.out.println(name + " 拿起了口红");
//                }
            }
            synchronized (lip){
                System.out.println(name + " 拿起了口红");
            }

        }
    }
}

//口红类
class Lipstick{

}

//镜子类
class Mirror{

}
