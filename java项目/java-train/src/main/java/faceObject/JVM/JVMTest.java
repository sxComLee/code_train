package faceObject.JVM;

/**
 * @ClassName JVMTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-25 09:33
 * @Version 1.0
 */
public class JVMTest {
    //static关键字修饰，类属性，放入方法区
    static String a = "123";
    //类属性，放入方法区
    String a1 = "234";

    //static 类方法，放入方法区
    static String getA(){
        return a;
    }
    //普通方法
    String getB(){
        return a1;
    }

    public static void main(String[] args) {
        //创建对象，在进行对象加载的时候，类相关的属性和方法全部加载到了方法区
        //同时在堆中开辟一块内存，通过main方法栈中的dataTest变量指向这块内存区域
        JVMTest dataTest = new JVMTest();
        //直接通过
        String a = JVMTest.a;
    }
}

