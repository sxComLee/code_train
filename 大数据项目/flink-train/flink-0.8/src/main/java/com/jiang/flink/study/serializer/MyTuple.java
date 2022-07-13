package com.jiang.flink.study.serializer;

import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * @ClassName MyTuple
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-20 20:54
 * @Version 1.0
 */
@Data
@TypeInfo(MyTupleTypeInfoFactory.class)
public class MyTuple<T0,T1> {
    public T0 myfield0;
    public T1 myfield1;
}
