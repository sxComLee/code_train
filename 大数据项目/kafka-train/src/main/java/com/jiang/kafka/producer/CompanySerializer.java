package com.jiang.kafka.producer;

import com.jiang.kafka.producer.bean.Company;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;


/**
 * @ClassName CompanySerializer
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-07-24 19:47
 * @Version 1.0
 */
public class CompanySerializer implements Serializer<Company> {
    //创建KafkaProducer实例的时候使用
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if(null == data){
            return null;
        }
        byte[] name,address;
        try{
            if(data.getName() != null){
                name = data.getName().getBytes("UTF-8");
            }else{
                name = new byte[0];
            }
            if(data.getAddress() != null){
                address = data.getAddress().getBytes("UTF-8");
            }else{
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);

            return buffer.array();

        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }

    public static Properties initCompanyConfig(){
        String brokerList = "localhost:9092";

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "client-id");
        prop.put(ProducerConfig.RETRIES_CONFIG, 5);
        return prop;
    }

    public static void main(String[] args) throws Exception {
        String topic = "topic-demo";
        Properties prop = initCompanyConfig();
        KafkaProducer<String, Company> produer = new KafkaProducer<>(prop);
        Company com = Company.builder().name("name").address("address").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, com);
        produer.send(record).get();
    }
}
