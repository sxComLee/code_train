package jvm;

/**
 * @ClassName MyObject
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-02-23 16:01
 * @Version 1.0
 */
public class MyObject {
    /**
     * @Author jiang.li
     * @Description //TODO 如果是自带的类加载器就是bootStrap ClassLoader
     *  如果是自己写的类加载器是 AppClassLoader
     * @Date 16:04 2020-02-23
     * @Param [args]
     * @return void
     **/
    public static void main(String[] args) {
        Object object = new Object();
        //null bootStrap ClassLoader是用c++写的，所以这里加载的时候就是null
        System.out.println(object.getClass().getClassLoader());

        MyObject myObject = new MyObject();
        //sun.misc.Launcher$AppClassLoader@18b4aac2，sun.misc.Launcher 是jvm相关的入口程序
        System.out.println(myObject.getClass().getClassLoader());
        //sun.misc.Launcher$ExtClassLoader@2f0e140b  ExtClassLoader 扩展类加载器, Java
        System.out.println(myObject.getClass().getClassLoader().getParent());
        // null 启动类加载器(Bootstrap ) c++
        System.out.println(myObject.getClass().getClassLoader().getParent().getParent());

        //jre中的rt.jar包 即runtime = rt，javax是java extend就java的拓展包（jre/lib/ext包下的jar包）
    }
}
