package com.jiang.flink.study.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName LogEvent
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-15 15:55
 * @Version 1.0
 */
@Data //@Data注解在类上，会为类的所有属性自动生成setter/getter、equals、canEqual、hashCode、toString方法，如为final属性，则不会为该属性生成setter方法。
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LogEvent {
    //the type of log(app、docker、...)
    private String type;

    // the timestamp of log
    private Long timestamp;

    //the level of log(debug/info/warn/error)
    private String level;

    //the message of log
    private String message;

    //the tag of log(appId、dockerId、machine hostIp、machine clusterName、...)
    private Map<String, String> tags = new HashMap<>();
}
