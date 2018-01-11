package com.offset;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.Constants;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *                       
 * @Filename MyConsume.java
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
 *   http://blog.csdn.net/qq_20641565/article/details/64440425
 */
public class MyConsume {
    private static Logger   LOG = LoggerFactory.getLogger(MyConsume.class);

    public MyConsume() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constants.kafka);
        //设置不自动提交，自己手动更新offset
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "lijieGroup");
        properties.put("zookeeper.connect", Constants.zookeeper);
        properties.put("auto.commit.interval.ms", "1000");
        ExecutorService executor = Executors.newFixedThreadPool(5);

        //执行消费
        for (int i = 0; i < 7; i++) {
            executor.execute(new ConsumerThreadNew(new KafkaConsumer<String, String>(properties), "mt007", "消费者" + (i + 1)));
        }
    }
}