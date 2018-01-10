package spark.kafka.kafka;

import com.Constants;

public interface KafkaProperties
{
    //final static String zkConnect = "10.111.134.60:2181";
    final static String zkConnect = Constants.zookeeper;
    final static String groupId1 = "test-consumer-group";
    final static String groupId2 = "group2";
    final static String topic = "mt02";
    //final static String kafkaServerURL = "master";
    final static int kafkaServerPort = 9092;
    final static int kafkaProducerBufferSize = 64 * 1024;
    final static int connectionTimeOut = 20000;
    final static int reconnectInterval = 10000;
    final static String topic2 = "topic2";
    final static String topic3 = "topic3";
    final static String clientId = "SimpleConsumerDemoClient";
}