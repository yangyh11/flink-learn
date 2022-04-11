//package com.yangyh.flink.connectors;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
//
//import java.time.Instant;
//import java.util.Random;
//import java.util.TimeZone;
//
//public class KafkaSinkDemo {
//
//    private static final Random random = new Random();
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<String> logMsg = env.addSource(new SourceFunction<String>() {
//            @Override
//            public void run(SourceContext<String> context) throws Exception {
//
//                while (true) {
//                    TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
//                    Instant instant = Instant.ofEpochMilli(System.currentTimeMillis() + tz.getOffset(System.currentTimeMillis()));
//
//                    String outline = String.format(
//                            "{\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\", \"behavior\": \"%s\", \"ts\": \"%s\"}",
//                            random.nextInt(10),
//                            random.nextInt(100),
//                            random.nextInt(1000),
//                            "pv",
//                            instant.toString());
//                    context.collect(outline);
//                    Thread.sleep(2000);
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });
//        logMsg.addSink(new FlinkKafkaProducer011<>(
//                "172.31.122.33:9092",
//                "yangyh_test",
//                new SimpleStringSchema()
//        )).name("flink-sink-kafka");
//
//        env.execute("FlinkSendKafka");
//    }
//}
