package com.jiang.flink.program.recommandSystemDemo.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @ClassName LogEntity
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 11:44
 * @Version 1.0
 */
@Getter
@Setter
public class LogEntity {
    private int userId;
    private int productId;
    private Long time;
    private String action;
}
