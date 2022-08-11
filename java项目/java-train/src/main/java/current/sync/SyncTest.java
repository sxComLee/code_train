package current.sync;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName SyncTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-26 15:00
 * @Version 1.0
 */
public class SyncTest {
    public static void main(String[] args) {
        Account d = new Account("豆豆", 100);
//        new Thread(new Drawing(80, d), "fang").start();
//        new Thread(new Drawing(40, d), "jiang").start();
        new Thread(new SyncDrawing(80, d), "fang").start();
        new Thread(new SyncDrawing(40, d), "jiang").start();
    }
}

/**
 * @Author jiang.li
 * @Description //TODO 模拟银行账户
 * @Date 15:05 2019-12-26
 * @Param 
 * @return 
 **/
@Data
@AllArgsConstructor
class Account{
    String aname;
    int totalMony;
}

class SyncDrawing implements Runnable{
    private int drawNum;
    private Account acc;
    private int expenseTotal;

    public SyncDrawing(int drawNum, Account ac){
        this.drawNum = drawNum;
        this.acc =ac;
    }

    @Override
    public void run() {
        draw();
    }

    void draw(){
        synchronized (acc){
            //判断现在还有多少钱，能不能取
            if(acc.totalMony < drawNum){
                System.out.println("钱不够了，现在账户余额 "+acc.getTotalMony()+" 要取 "+drawNum);
                return;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            acc.totalMony -= drawNum;
            expenseTotal += drawNum;
            System.out.println(Thread.currentThread().getName() + "--账户余额：" + acc.getTotalMony());
            System.out.println(Thread.currentThread().getName() + "--总共取了：" + expenseTotal);
        }
    }

}

class Drawing implements Runnable{
    private int drawNum;
    private Account acc;
    private int expenseTotal;

    public Drawing(int drawNum, Account ac){
        this.drawNum = drawNum;
        this.acc =ac;
    }

    @Override
    public void run() {
        //判断现在还有多少钱，能不能取
        if(acc.totalMony < drawNum){
            return;
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        acc.totalMony -= drawNum;
        expenseTotal += drawNum;
        System.out.println(Thread.currentThread().getName() + "--账户余额：" + acc.getTotalMony());
        System.out.println(Thread.currentThread().getName() + "--总共取了：" + expenseTotal);
    }
}
