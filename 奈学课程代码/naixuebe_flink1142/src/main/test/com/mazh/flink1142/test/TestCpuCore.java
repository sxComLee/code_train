package com.mazh.flink1142.test;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 */
public class TestCpuCore {

    public static void main(String[] args) {

        int cpu = Runtime.getRuntime().availableProcessors();
        System.out.println("cpu个数：" + cpu);
    }
}
