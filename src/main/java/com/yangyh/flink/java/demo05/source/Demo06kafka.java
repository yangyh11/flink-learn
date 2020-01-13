package com.yangyh.flink.java.demo05.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @description: Flink可以读取Kafka数据
 * @author: yangyh
 * @create: 2020-01-09 20:52
 */
public class Demo06kafka {

    static String topic = "FlinkTopic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092, node2:9092, node3:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");

        // 从kafka消费数据
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);

        // Flink读取kafkad的数据
        DataStreamSource<String> streamSource = env.addSource(consumer011);

        streamSource.print();

        env.execute();

    }
}
