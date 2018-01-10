package com.kafka;
import java.util.Properties;

import com.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
import org.apache.log4j.Logger;
 
/**
 * <p> Copyright: Copyright (c) 2015 </p>
 * 
 * <p> Date : 2015-11-17 21:42:50 </p>
 * 
 * <p> Description : JavaApi for kafka producer </p>
 *
 * @author luchunli
 * 
 * @version 1.0
 *
 */
public class SimpleKafkaProducer {
    private static final Logger logger = Logger.getLogger(SimpleKafkaProducer.class);
    /**
     * 
     */
    private void execMsgSend() {
        Properties props = new Properties();
        props.put("metadata.broker.list", Constants.kafka);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
         
        ProducerConfig config = new ProducerConfig(props); 
         
        logger.info("set config info(" + config + ") ok.");
         
        Producer<String, String> procuder = new Producer<>(config);
         
        String topic = "cctopic";
        for (int i = 1; i <= 10; i++) {
            String value = "value_" + i;
            KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, value);
            procuder.send(msg);
        }
        logger.info("send message over.");
             
        procuder.close();
    }
     
    /**
     * @param args
     */
    public static void main(String[] args) {
        SimpleKafkaProducer simpleProducer = new SimpleKafkaProducer();
        simpleProducer.execMsgSend();
    }
 
}