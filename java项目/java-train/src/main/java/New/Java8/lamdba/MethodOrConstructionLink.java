package New.Java8.lamdba;

import javax.swing.*;

/**
 * Description: 方法引用与构造器引用
 * 如果lambda 表示大代码块只有一条代码，可以省略花括号，在代码块中使用方法引用和构造器引用
 *
 * @author lij
 * @date 2022-01-25 23:14
 */
public class MethodOrConstructionLink {

    public static void main(String[] args) {
        // classMethod();
        // objectMethod();
        construction();
    }

    /**
     *  引用类方法
     */
    public static void classMethod(){
        // 正常写法
        System.out.println("=================> 引用类方法演示 <=================");
        Converter converter = from -> Integer.valueOf(from);
        Integer result = converter.convert("99");
        System.out.println("优化之前的结果...."+result);

        // 方法引用代替lambda表达式，引用类方法
        converter = Integer :: valueOf;
        result = converter.convert("199");
        System.out.println("优化之后的结果...."+result);
    }

    /**
     *  引用特定对象的实例方法
     */
    public static void objectMethod(){
        System.out.println("=================> 引用特定对象的实例方法演示 <=================");
        Converter converter = from -> "fkit.org".indexOf(from);
        Integer result = converter.convert("it");
        System.out.println("替换之前的结果。。。"+result);

        // 方法引用代替 lambda 表达式，引用特定对象的实例方法
        converter = "fkit.org"::indexOf;
        result = converter.convert("it");
        System.out.println("替换之后的结果。。。"+result);
    }

    /**
     *  引用类对象的实例方法
     */
    public static void classObjectMethod(){
        System.out.println("=================> 引用某类对象的实例方法演示 <=================");
        MyTest mt = (a,b,c) -> a.substring(b,c);
        String str = mt.test("I love doudou", 2, 7);
        System.out.println("替换之前结果。。。"+str);

        //函数式接口中被实现方法的第一个参数作为调用者
        mt = String :: substring;
        str = mt.test("I love my wife",2,9);
        System.out.println("替换之后结果。。。"+str);
    }

    /**
     *  引用构造器
     */
    public static void construction(){
        System.out.println("=================> 引用构造器演示 <=================");
        YourTest yt = a -> new JFrame(a);
        JFrame jf = yt.win("我的窗口");
        System.out.println("优化之前结果。。。"+jf);

        yt = JFrame :: new;
        jf = yt.win("我的窗口2");
        System.out.println("优化之后的结果。。。"+jf);
    }

    @FunctionalInterface
    interface Converter{
        Integer convert(String num);
    }

    @FunctionalInterface
    interface MyTest{
        String test(String a,int b,int c);
    }

    @FunctionalInterface
    interface YourTest{
        JFrame win(String title);
    }
}
