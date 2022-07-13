package jvm;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Main
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-02-22 17:31
 * @Version 1.0
 */
public class Main {
    public static void main(String[] args) {
        List<Demo> demoList = new ArrayList<>();
        while(true){
            demoList.add(new Demo());
        }
    }
}
