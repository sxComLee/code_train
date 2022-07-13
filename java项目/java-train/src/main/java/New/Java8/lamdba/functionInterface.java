package New.Java8.lamdba;

/**
 * Description: 函数式接口使用案例
 *  函数接口指的是只有一个函数的接口，这样的接口可以隐式转换为Lambda表达式
 *  lambda 表达式的类型被称为目标类型，必须是函数式接口（functional interface）：可以包含多个默认方法，类方法，但是只能有一个抽象方法
 *  @FunctionalInterface 编译器严格检查是否为函数式接口
 * @author lij
 * @date 2022-01-25 08:36
 */
@FunctionalInterface
public interface functionInterface {

    void method();

    // 默认方法和静态方法不会破坏函数式接口的定义，因此如下的代码是合法的
    default void defaultMethod() {
    }

    static void staticMethod(){

    }
}
