package com.partition;

import java.util.Properties;
import java.util.UUID;

import com.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 
 * @author Lijie
 *
 */
public class MyProducerA {

    public static void main(String[] args) throws Exception {
        produce();
    }

    public static void produce() throws Exception {

        //topic
        String topic = "mt005";

        //配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constants.kafka);

        //序列化类型
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者
        KafkaProducer<String, String> pro = new KafkaProducer<>(properties);
        while (true) {

            //模拟message
            String value = UUID.randomUUID().toString();

            //封装message
            ProducerRecord<String, String> pr = new ProducerRecord<String, String>(topic, value);

            //发送消息
            pro.send(pr);
            System.out.println("发送消息："+value);
            //sleep
            Thread.sleep(1000);
        }
    }
}