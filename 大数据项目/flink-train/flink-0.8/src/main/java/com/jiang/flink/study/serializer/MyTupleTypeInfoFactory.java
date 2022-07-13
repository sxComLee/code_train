package com.jiang.flink.study.serializer;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * @ClassName MyTupleTypeInfoFactory
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-20 21:18
 * @Version 1.0
 */
public class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {

    @Override
    public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return null;
//        return new MyTupleTypeInfoFactory(genericParameters.get("T0"),genericParameters.get("T1"));
    }
}
