package reflect;

/**
 * @ClassName Demo01
 * @Description TODO 测试各种类型（class,interface,enum,annotation,primitive type,void）对应的java.lang.Class对象的获取方式
 * @Author jiang.li
 * @Date 2020-01-31 21:09
 * @Version 1.0
 */
@SuppressWarnings("all")
public class Demo01 {
    public static void main(String[] args) {
        String path = "reflect.bean.User";
        try {
            Class clazz = Class.forName(path);
            //对象是表示或封装一些数据，一个类被加载后，JVM会创建一个对应类的Class对象，类的整个结构信息会放到对应的Class对象中。
            //这个Class对象就像一面镜子，通过这面镜子就可以看到对应累的全部信息
            System.out.println(clazz.hashCode());


            Class clazz2 = Class.forName(path);
            //两次的hashcode是一样的，所以，实际上是一个对象，即一个类只对应了一个class对象——单例
            System.out.println(clazz.hashCode());


            //获取class对象的方法
            Class<String> class1 = String.class;
            Class<? extends String> class2 = path.getClass();
            Class<Integer> class3 = int.class;
            System.out.println(class1 == class2);


            //对于数组对象，判断是否相同是根据数组的维数而不是长度
            int[] arr1 = new int[10];
            int[] arr2 = new int[30];
            //相同
            System.out.println(arr1.getClass().hashCode());
            System.out.println(arr2.getClass().hashCode());

            int[] arr01 = new int[10];
            int[][] arr02 = new int[30][10];
            //不同
            System.out.println(arr01.getClass().hashCode());
            System.out.println(arr02.getClass().hashCode());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
