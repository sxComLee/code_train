package current.sync;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Description:
 *
 * @author lij
 * @date 2022-07-15 20:52
 */
public class LockAccount {
    // 可重入锁对象，一个线程对已被加锁的 ReentrantLock 锁再次加锁，
    //  ReentrantLock对象会维持一个计数器来追踪lock方法的嵌套调用
    // 线程在每次调用lock()加锁后, 必须显式调用unlock()来释放锁,
    //  所以一段被锁保护的代码可以调用另一个被相同锁保护的方法。
    private final ReentrantLock lock = new ReentrantLock();

    // 封装账户编号，账户余额的两个成员变成
    private String accountNo;
    private double balance;

    public LockAccount(){}

    public LockAccount(String accountNo,double balance){
        this.accountNo = accountNo;
        this.balance = balance;
    }

    // 省略accountNo的get和set方法

    // 因为账户余额不允许随便修改，所以只为balance提供getter方法
    public double getBalance(){
        return this.balance;
    }

    // 提供一个线程安全的draw()  方法来完成取钱操作
    public void draw(double drawAmount){
        // 加锁
        lock.lock();
        try{
            // 账户余额大于可取钱数
            if(balance >= drawAmount){
                // 吐出钞票
                System.out.println(Thread.currentThread().getName()+"取钱成功！吐出钞票："+drawAmount);

                try{
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

                //修改余额
                balance -= drawAmount;
                System.out.println("\t 余额为："+balance);
            }else{
                System.out.println(Thread.currentThread().getName()+"取钱失败！余额不足");
            }

        }finally {
            //修改完成，释放锁
            lock.unlock();
        }
        // 省略 hashCode() 和 equals() 方法
    }
}
