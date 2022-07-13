package com.jiang.flink.study.source.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName Rule
 * @Description TODO 规则
 * @Author jiang.li
 * @Date 2019-12-16 14:12
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Rule {
    /**
     * rule id
     */
    private String id;

    /**
     * rule name
     */
    private String name;

    /**
     * rule type
     */
    private String type;

    /**
     * monitor measurement
     */
    private String measurement;

    /**
     * rule expression
     */
    private String expression;

    /**
     * measurement threshold
     */
    private String threshold;

    /**
     * alert level
     */
    private String level;

    /**
     * rule targetType
     */
    private String targetType;

    /**
     * rule targetId
     */
    private String targetId;

    /**
     * notice webhook, only DingDing group rebot here
     * TODO: more notice ways
     */
    private String webhook;

}
