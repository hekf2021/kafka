package com.offset;
import java.util.Properties;

import com.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 
 *                       
 * @Filename MyProducer.java
 *
 * @Description 
 *
 * @Version 1.0
 *
 * @Author Lijie
 *
 * @Email lijiewj39069@touna.cn
 *       
 * @History
 *<li>Author: Lijie</li>
 *<li>Date: 2017年3月21日</li>
 *<li>Version: 1.0</li>
 *<li>Content: create</li>
 *  http://blog.csdn.net/qq_20641565/article/details/64440425
 */
public class MyProducer {

    private static Properties                       properties;
    private static KafkaProducer<String, String>    pro;
    static {
        //配置
        properties = new Properties();
        properties.put("bootstrap.servers", Constants.kafka);
        //序列化类型
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者
        pro = new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws Exception {
        produce("mt004");
    }

    public static void produce(String topic) throws Exception {

        //模拟message
        //          String value = UUID.randomUUID().toString();
        for (int i = 0; i < 10000; i++) {
            //封装message
            ProducerRecord<String, String> pr = new ProducerRecord<String, String>(topic, "哈哈哈哈哈哈哈哈"+i);

            //发送消息
            pro.send(pr);
            Thread.sleep(1000);
        }

    }
}