package com.jiang.flink.study.source.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName Student
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-16 14:14
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;
}
