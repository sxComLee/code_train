package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProduceMessages {
    public static void main(String[] args) throws InterruptedException {
        int messageNum = 0;
        String messageStr = "";
        boolean isAsync = true;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        int i =0;
        while(true){
            long startTime = System.currentTimeMillis();
            messageNum ++;
            messageStr = messageStr+ messageNum;
            ProducerRecord<String, String> record = new ProducerRecord<>("zp", messageNum + "", "world"+ i);
            if(isAsync){
                //发送数据
                producer.send(record,new DemoCallBack(startTime,messageNum,messageStr));
                Thread.sleep(1000);
            }else{
                try{
                    //发送数据
                    producer.send(record);
                    Thread.sleep(1000);
                }catch (InterruptedException exception){
                    exception.printStackTrace();
                }
            }
        }
    }
}
