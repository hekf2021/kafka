package com.offset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *                       
 * @Filename ConsumerThreadNew.java
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
 *
 */
public class ConsumerThreadNew implements Runnable {
    private static Logger                   LOG = LoggerFactory.getLogger(ConsumerThreadNew.class);

    //KafkaConsumer kafka生产者
    private KafkaConsumer<String, String>   consumer;

    //消费者名字
    private String                          name;

    //消费的topic组
    private List<String>                    topics;

    //构造函数
    public ConsumerThreadNew(KafkaConsumer<String, String> consumer, String topic, String name) {
        super();
        this.consumer = consumer;
        this.name = name;
        this.topics = Arrays.asList(topic);
    }

 
    public void run() {
        consumer.subscribe(topics);
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();

        // 批量提交数量
        final int minBatchSize = 1; 
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //LOG.info("消费者的名字为:" + name + ",消费的消息为：" + record.value());
                System.out.println("消费者的名字为:" + name + ",消费的消息为：" + record.value());
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //这里就是处理成功了然后自己手动提交
                consumer.commitSync();
                //LOG.info("提交完毕");
                System.out.println("提交完毕");
                buffer.clear();
            }
        }
    }

}