package annotation;


import annotation.demo.Table;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * @ClassName ReflactAnnotation
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-31 18:53
 * @Version 1.0
 */
public class ReflectAnnotation {
    public static void main(String[] args) throws Exception {
        Class<?> clazz = Class.forName("annotation.demo.Student");
        //获得类的所有有效注解
        Annotation[] annotations = clazz.getAnnotations();

        for (Annotation a: annotations) {
            System.out.println(a);
        }

        Table annotation = clazz.getAnnotation(Table.class);
        System.out.println(annotation.value());

        /*获取属相关的注解*/
        Field name = clazz.getDeclaredField("name");
        annotation.demo.Field field = name.getAnnotation(annotation.demo.Field.class);
        System.out.println(field.columName()+"---->"+field.columName()
                +"---->"+field.type()
                +"---->"+field.length());

        //根据获得的表名，字段信息等可以拼出DDL语句，然后，使用JDBC执行SQL，
    }

}
