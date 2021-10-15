package com.example.kafkatest;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;
/**
 * 自定义分区
 */
public class CustomPartitioner implements Partitioner {

    public CustomPartitioner() {
        super();

    }

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
