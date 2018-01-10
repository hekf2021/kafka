package com.create;

import com.Constants;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.security.JaasUtils;
import scala.Function1;
import scala.collection.Iterable;

import javax.naming.NamingEnumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * https://www.cnblogs.com/huxi2b/p/6592862.html
 */
public class CRUD {
    public static void main(String[] args){
        ZkUtils zkUtils=null;
        try {
            zkUtils = ZkUtils.apply(Constants.zookeeper, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            createTopic(zkUtils,"mt005");
            //findTopic(zkUtils,"mt003");
            //dropTopic(zkUtils,"mt003");


        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            zkUtils.close();
        }
    }

    public static void createTopic(ZkUtils zkUtils,String topic){
        // 创建一个5区单3副本名为t1的topic
        AdminUtils.createTopic(zkUtils, topic, 3, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
    }

    public static void findTopic(ZkUtils zkUtils,String topic){
        // 获取topic 'test'的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        // 查询topic-level属性
        Iterator it = props.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry=(Map.Entry)it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + " = " + value);
        }
    }

    public static void dropTopic(ZkUtils zkUtils,String topic){
        AdminUtils.deleteTopic(zkUtils, topic);
    }

    public static void updateTopic(ZkUtils zkUtils,String topic){
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        // 增加topic级别属性
        props.put("min.cleanable.dirty.ratio", "0.3");
       // 删除topic级别属性
        props.remove("max.message.bytes");
        // 修改topic 'test'的属性
        AdminUtils.changeTopicConfig(zkUtils, topic, props);
    }


}
