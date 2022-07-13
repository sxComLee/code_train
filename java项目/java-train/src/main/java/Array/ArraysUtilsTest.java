package Array;

import java.util.Arrays;

/**
 * @ClassName ArraysTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-25 15:08
 * @Version 1.0
 */
public class ArraysUtilsTest {
    public static void main(String[] args) {
        String[] s1 = {"1","2","3","4"};
        int i = Arrays.binarySearch(s1, "2");
        System.out.println(i);
    }
}
