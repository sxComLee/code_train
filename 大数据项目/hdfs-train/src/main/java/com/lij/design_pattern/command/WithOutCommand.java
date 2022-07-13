package com.lij.design_pattern.command;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 命令设计模式
 */
public class WithOutCommand {

    public static void main(String[] args) {
        int opCode = 1;
        if (opCode == 1) {
            OperateUtil.read();
        } else if (opCode == 2) {
            OperateUtil.write();
        }
    }

    // TODO_MA 马中华 注释： 操作
    static class OperateUtil {

        public static void read() {
            System.out.println("Read Operation");
        }

        public static void write() {
            System.out.println("Write Operation");
        }
    }
}


