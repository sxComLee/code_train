package com.jiang.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Properties;

/**
 * @ClassName ProducerInterceptorPrefix
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-07-24 20:46
 * @Version 1.0
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor {
    private volatile long sendSuccess = 0;
    private volatile long sendFfiled = 0;

    //序列化和计算分区前调用
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String modifyValue = "prefix-1"+record.key();
        return new ProducerRecord(record.topic(),modifyValue);
    }

    //消息被应答之前或者发送失败时调用
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(metadata == null){
            sendFfiled ++;
        }else{
            sendSuccess ++ ;
        }
    }

    //关闭拦截器时执行一些资源清理工作
    @Override
    public void close() {
        double successRatio = (double)sendSuccess / (sendSuccess + sendFfiled);
        System.out.println("发送成功率为："+successRatio);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    public static Properties initInterceptorConfig(){
        String brokerList = "localhost:9092";

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomerPartitioner.class.getName());
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName() + "," + ProducerInterceptorSecond.class.getName() );
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "client-id");
        prop.put(ProducerConfig.RETRIES_CONFIG, 5);
        return prop;
    }


}

class ProducerInterceptorSecond implements ProducerInterceptor{

    //序列化和计算分区前调用
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String modifyValue = "prefix-2"+record.key();
        return new ProducerRecord(record.topic(),modifyValue);
    }

    //消息被应答之前或者发送失败时调用
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    //关闭拦截器时执行一些资源清理工作
    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }


}
