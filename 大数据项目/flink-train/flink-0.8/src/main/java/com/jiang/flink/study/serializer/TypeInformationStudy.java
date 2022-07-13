package com.jiang.flink.study.serializer;

import com.jiang.flink.program.bean.RegionInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * @ClassName TypeInformation
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-20 20:47
 * @Version 1.0
 */
public class TypeInformationStudy {
    public static void main(String[] args) {
        //============== 声明类型 ========
        //非泛型直接引用
        TypeInformation<RegionInfo> of2 = TypeInformation.of(RegionInfo.class);
        //泛型需要指定
        TypeInformation<Tuple2<String, String>> of3 = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });

        //预定义消息常量
        TypeInformation<String> of = TypeInformation.of(new TypeHint<String>() {
        });
        TypeInformation<String> of1 = Types.STRING;

        //============== 注册子类型 ========
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //注册子类信息
        env.registerType(MyTuple.class);

        //============== kryo序列化 ========

        //强制使用avro序列化
        env.getConfig().enableForceAvro();
        env.getConfig().enableForceKryo();

        //为kryo增加自定义的Seralizer以及增强Kryp的功能
//        env.getConfig().addDefaultKryoSerializer();
        //禁用kryo
        env.getConfig().disableGenericTypes();
        env.getConfig().disableForceKryo();

    }


}


