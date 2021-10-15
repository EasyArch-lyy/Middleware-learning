package com.example.kafkatest.consumerTest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 定义 kakfa 服务的地址，不需要将所有 broker 指定上
        // StreamsConfig.BOOTSTRAP_SERVERS_CONFIG，它指定用于建立与 Kafka 集群的初始连接的主机/端口对列表
        props.put("bootstrap.servers", "192.168.119.128:9092");
        // 制定 consumer group
        props.put("group.id", "test");
        // 是否自动确认 offset
        props.put("enable.auto.commit", "true");
        // 自动确认 offset 的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key 的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value 的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义 consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费订阅的topic，可同时订阅多个
        consumer.subscribe(Arrays.asList("testTopic", "first"));
        while (true){
            //读取数据，超时时间为100ms
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record: records) {
                System.out.println("offset = " + record.offset() + ", key =" + record.key() + ", value = " + record.value());
            }
        }
    }
}
