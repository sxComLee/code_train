package annotation.demo;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * @ClassName Field
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-31 17:04
 * @Version 1.0
 */
@Target({TYPE,FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Field {
    String columName();
    String type() default "varchar";
    int length() default 10;

}
