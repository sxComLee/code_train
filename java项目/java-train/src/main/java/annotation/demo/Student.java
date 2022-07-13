package annotation.demo;

/**
 * @ClassName Student
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-31 17:00
 * @Version 1.0
 */
@Table("t_student")
public class Student {
    @Field(columName = "id",type="int")
    String id;
    @Field(columName = "age",length = 3)
    int age;
    @Field(columName = "sname")
    String name;

}
