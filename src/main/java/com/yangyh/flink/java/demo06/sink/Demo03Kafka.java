package com.yangyh.flink.java.demo06.sink;

import com.yangyh.flink.java.demo05.source.Demo05NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @description: Flink将数据写往Kafka
 * @author: yangyh
 * @create: 2020-01-09 21:22
 */
public class Demo03Kafka {

    static String topic = "FlinkTopic";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> streamSource = streamEnv.addSource(new Demo05NoParallelSource());

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return value.toString();
            }
        });

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092, node2:9092, node3:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        FlinkKafkaProducer011<String> producer011 = new FlinkKafkaProducer011<>(topic, new SimpleStringSchema(), properties);

        // Flink将数据写往Kafka
        map.addSink(producer011);

        streamEnv.execute();

    }
}
