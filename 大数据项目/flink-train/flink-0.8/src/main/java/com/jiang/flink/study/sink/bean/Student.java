package com.jiang.flink.study.sink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName Student
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-07 19:10
 * @Version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;
}
