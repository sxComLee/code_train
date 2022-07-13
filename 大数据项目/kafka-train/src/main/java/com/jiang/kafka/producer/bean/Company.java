package com.jiang.kafka.producer.bean;

import lombok.*;

/**
 * @ClassName Company
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-07-24 19:48
 * @Version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Company {
    private String name;
    private String address;

}
