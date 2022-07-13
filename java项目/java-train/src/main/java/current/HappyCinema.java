package current;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;

/**
 * @ClassName HappyCinema
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-26 16:47
 * @Version 1.0
 */
public class HappyCinema {
    public static void main(String[] args) {
        ArrayList<Integer> seats = new ArrayList<>();
        seats.add(1);
        seats.add(2);
        seats.add(3);
        seats.add(4);
        seats.add(5);
        seats.add(6);
        Cinema happy = new Cinema(seats, "happy");
        ArrayList<Integer> book1 = new ArrayList<>();
        book1.add(1);
        book1.add(2);
        book1.add(3);
        new Thread(new BookTicket(happy,book1),"高").start();

        ArrayList<Integer> book2 = new ArrayList<>();
//        book2.add(1);
//        book2.add(3);
        book2.add(4);
        new Thread(new BookTicket(happy,book2),"李").start();

    }


}

@AllArgsConstructor
@Data
class Cinema{
    private ArrayList<Integer> remainSeats;
    private String name;
}

@AllArgsConstructor
class BookTicket implements Runnable{
    private Cinema cinema;
    private ArrayList<Integer> needSeats;

    @Override
    public void run() {

//        synchronized (cinema){
            System.out.println("可用位置"+cinema.getRemainSeats());
            boolean book = book(needSeats);
            if(book){
                System.out.println("出票成功位置："+needSeats);
            }else{
                System.out.println("出票失败位置："+needSeats);
            }
//        }


    }

    boolean book(ArrayList<Integer> needSeats) {
        ArrayList<Integer> remainSeats = cinema.getRemainSeats();
        //先判断当前票是否足够
        if(remainSeats.size()<=0){
            return false;
        }
        synchronized (remainSeats){
            if(remainSeats.size()<=0){
                return false;
            }

            ArrayList<Integer> copy = new ArrayList<>();
            copy.addAll(remainSeats);
            copy.removeAll(needSeats);
            if(copy.size() != (remainSeats.size()- needSeats.size())){
                return false;
            }
            remainSeats = copy;
            cinema.setRemainSeats(remainSeats);
            return true;
        }


    }
}
