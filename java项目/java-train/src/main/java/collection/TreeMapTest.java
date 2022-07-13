package collection;

import java.util.TreeMap;

/**
 * @ClassName TreeMapTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-26 11:14
 * @Version 1.0
 */
public class TreeMapTest {
    public static void main(String[] args) {
        TreeMap<Integer,String> treeMap = new TreeMap();
        treeMap.put(20,"20");
        treeMap.put(40,"40");
        treeMap.put(30,"30");


        System.out.println(treeMap);
    }

}
