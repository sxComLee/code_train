package proxy.InvocationHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @ClassName Helloimplements
 * @Description TODO
 *  由于java封装了newProxyInstance这个方法的实现细节，所以使用起来才能这么方便
 *  静态代理和JDK代理有一个共同的缺点，就是目标对象必须实现一个或多个接口，假如没有，则可以使用Cglib代理。
 *
 * @Author jiang.li
 * @Date 2020-01-15 16:57
 * @Version 1.0
 */
public class Helloimplements implements IHello {

    @Override
    public void sayHello(String name) {
//        System.out.println("问候之前的日志记录...");
        System.out.println("hello "+name);
    }

    @Override
    public void sayGoogBye(String name) {
//        System.out.println("问候之前的日志记录...");
        System.out.println(name + "good Bye" );
    }

    public static void main(String[] args) {
        Helloimplements hello = new Helloimplements();
        Helloimplements proxy = (Helloimplements)Proxy.newProxyInstance(hello.getClass().getClassLoader()
                , hello.getClass().getInterfaces(), new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        System.out.println("方法执行之前");
                        Object redultValue = method.invoke(hello, args);
                        System.out.println("方法执行之后");
                        return redultValue;
                    }
                });

        proxy.sayGoogBye("name");
        proxy.sayHello("name");
    }
}
