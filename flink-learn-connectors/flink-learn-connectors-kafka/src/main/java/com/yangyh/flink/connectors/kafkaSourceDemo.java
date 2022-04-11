//package com.yangyh.flink.connectors;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.KafkaClient;
//import org.apache.kafka.clients.producer.KafkaProducer;
//
//import java.util.Properties;
//
//@Slf4j
//public class kafkaSourceDemo {
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // kafka配置
//        Properties properties = new Properties();
//        properties.setProperty(KafkaClient, "127.0.0.1:9092");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("group.id", ConfigUtil.getValue("kafka.consumer.group.id.SinkAdbJob"));
//        properties.setProperty("enable.auto.commit", "true");
//        properties.setProperty("auto.commit.interval.ms", "5000");
//
//        DataStreamSource<String> wordStream = env.addSource(new SourceFunction<String>() {
//            @Override
//            public void run(SourceContext<String> sourceContext) throws Exception {
//                while (true) {
//                    String  word = String.valueOf((char)(Math.random()*26 + 'a'));
//                    log.info("random word=" + word);
//                    sourceContext.collect(word);
//                    Thread.sleep(1000);
//                }
//            }
//            @Override
//            public void cancel() {
//
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountTp2Stream = wordStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                collector.collect(Tuple2.of(s, 1));
//            }
//        });
//
//        wordCountTp2Stream.keyBy(tp2 -> tp2.f1).sum(1).print();
//
//        new KafkaProducer<>()
//
//        env.execute("");
//
//    }
//}
