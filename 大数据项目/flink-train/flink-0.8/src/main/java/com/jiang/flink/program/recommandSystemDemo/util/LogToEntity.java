package com.jiang.flink.program.recommandSystemDemo.util;

import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import lombok.extern.slf4j.Slf4j;

/**
 * @author XINZE
 */
@Slf4j
public class LogToEntity {

    public static LogEntity getLog(String s){
        log.info(s);
        String[] values = s.split(",");
        if (values.length < 2) {
            System.out.println("Message is not correct");
            return null;
        }
        LogEntity log = new LogEntity();
        log.setUserId(Integer.parseInt(values[0]));
        log.setProductId(Integer.parseInt(values[1]));
        log.setTime(Long.parseLong(values[2]));
        log.setAction(values[3]);

        return log;
    }
}
