package com.lij.design_pattern.command;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 命令设计模式
 */
public class WithCommandPattern {

    public static void main(String[] args) {

        // TODO_MA 马中华 注释： 读操作
        Context context = new Context(new ReadCommand());
        context.execute();

        // TODO_MA 马中华 注释： 写操作
        Context context1 = new Context(new WriteCommand());
        context1.execute();
    }
}

interface Command {
    void execute();
}

class ReadCommand implements Command {
    @Override
    public void execute() {
        System.out.println("Read Operation");
    }
}

class WriteCommand implements Command {
    @Override
    public void execute() {
        System.out.println("Write Operation");
    }
}

class Context {
    private Command command;

    public Context(Command command) {
        this.command = command;
    }

    public void execute() {
        this.command.execute();
    }
}