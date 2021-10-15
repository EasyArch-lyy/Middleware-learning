package com.example.kafkatest.producerTest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 创建生产者发送消息带回调函数
 */
public class CallBackProducer {

    public static void main(String[] args) {

        Properties prop = new Properties();
        // Kafka服务端的主机名和端口号
        prop.put("bootstrap.servers", "192.168.119.128:9092");
        // 等待所有副本节点的应答
        prop.put("acks", "all");
        // 消息发送最大尝试次数
        prop.put("retries", 0);
        // 一批消息处理大小
        prop.put("batch.size", 16384);
        // 增加服务端请求延时
        prop.put("linger.ms", 1);
        // 发送缓存区内存大小
        prop.put("buffer.memory", 33554432);
        // key序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);
        for (int i = 0; i < 50; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("second", "bye" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println(metadata.partition() + "---" + metadata.offset());
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
