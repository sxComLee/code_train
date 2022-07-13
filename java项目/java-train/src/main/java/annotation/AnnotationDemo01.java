package annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(value={ANNOTATION_TYPE,CONSTRUCTOR})//在什么地方生效
@Retention(value=RUNTIME)
public @interface AnnotationDemo01 {
    String studentName() default "";
    int age() default 0;
    int id() default -1;// -1表示没有实际意义
    String[] school() default {"太原科技","太原理工"};
}
