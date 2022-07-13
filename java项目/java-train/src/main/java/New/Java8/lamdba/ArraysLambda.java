package New.Java8.lamdba;

import java.util.Arrays;

/**
 * Description:  lambda 表示基本使用方法
 *  函数式编程：闭包，将函数当成参数传递给某个方法，或者把代码本身当做数据处理
 *  之前是匿名内部类代替Lambda表达式
 * @author lij
 * @date 2022-01-25 08:22
 */
public class ArraysLambda {
    public static void main(String[] args) throws Exception{
        ArraysLambda();
    }

    public static void ArraysLambda(){
        String[] arr = new String[]{
                "java","kafka","hadoop","flink","yarn"
        };
        // 排序
        Arrays.parallelSort(arr,(a,b)-> a.length() - b.length());
        System.out.println("排序之后的结果。。。"+Arrays.toString(arr));

        // 根据索引重新赋值
        Arrays.parallelSetAll(arr,a-> arr[a]+"5");
        System.out.println("之后的结果。。。"+Arrays.toString(arr));

        // 处理前后的数据
        Arrays.parallelPrefix(arr,(left,right)-> left+right);
        System.out.println("增加前缀之后结果..."+Arrays.toString(arr));

    }

    public static void ClassMemberAndLocalVariable(){
        // 由逗号分隔的参数列表、->符号和语句块组成,参数e的类型是由编译器推理得出
        Arrays.asList("a","b","c","d").forEach( e-> System.out.println(e));

        // 显式指定参数类型
        Arrays.asList("a","b","c","d").forEach( (String e) -> System.out.println(e));

        // 复杂语句块
        Arrays.asList("a","b","c","d").forEach( e-> {
            System.out.println(e);
            System.out.println(e);
        });

        // 可以引用类成员和局部变量（会将这些变量隐式得转换成final的）
        String separator = ",";
        Arrays.asList("a","b","c","d").forEach( e-> System.out.println(e+separator));
        //  等价于
        final String separatorFix = ".";
        Arrays.asList("a","b","c","d").forEach( e-> System.out.println(e+separatorFix));

        // 如果有返回值，类型可以由编译器推理得出
        // 如果返回值只有一行，可以不用return
        Arrays.asList("a","b","c","d").sort((e1,e2) -> e1.compareTo(e2));
        // 等价于
        Arrays.asList("a","b","c","d").sort((e1,e2) -> {
            return e1.compareTo(e2);
        });
    }
}
