package collection;

/**
 * @ClassName GenericTypeTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-25 17:35
 * @Version 1.0
 */
public class GenericTypeTest {
    public static void main(String[] args) {
        MyCollection<String> s1 = new MyCollection<String>();
        String s = s1.get(0);
        System.out.println(s);
    }


}

class MyCollection<T>{
    Object[] objects = new Object[5];

    public T get(int index){
        return (T)objects[index];
    }

    public void set(T e,int index){
        objects[index] = e;
    }
}
