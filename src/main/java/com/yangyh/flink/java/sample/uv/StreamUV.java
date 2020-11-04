package com.yangyh.flink.java.sample.uv;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @description:
 * @author: yangyh
 * @create: 2020-11-05 00:34
 */
public class StreamUV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // nk -lk 8888
        DataStreamSource<String> streamSource = streamEnv.socketTextStream("node4", 8888);


        SingleOutputStreamOperator<String> vinStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> vinAndOne = vinStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String vin) throws Exception {
                return Tuple2.of(vin, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = vinAndOne.keyBy(1);


        SingleOutputStreamOperator<Integer> map = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Integer>() {

            // 使用KeyState
            private transient ValueState<HashSet> vinState;

            @Override
            public void open(Configuration parameters) {
                // 定义一个状态描述器
                ValueStateDescriptor<HashSet> stateDescriptor = new ValueStateDescriptor<HashSet>(
                        "vin-state",
                        HashSet.class
                );
                // 使用RuntimeContext获取状态
                vinState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Integer map(Tuple2<String, Integer> tuple2) throws Exception {
                String vin = tuple2.f0;
                HashSet vinSet = vinState.value();
                if(vinSet == null) {
                    vinSet = new HashSet<>();
                }
                vinSet.add(vin);
                // 更新状态
                vinState.update(vinSet);
                return vinSet.size();
            }
        });

        map.print();

        streamEnv.execute("单词UV统计");

    }
}
