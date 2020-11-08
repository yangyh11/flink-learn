package com.yangyh.flink.java.demo06.sink.hbase;

import com.google.common.base.Joiner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: 把wordCount的结果写入HBase
 * @author: yangyh
 * @create: 2020-11-08 23:17
 * HBase建表语句：
 * create 'word', {NAME => 'cf'}
 */
public class StreamToHBase {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // nk -lk 8888
        DataStreamSource<String> streamSource = streamEnv.socketTextStream("node4", 8888);


        SingleOutputStreamOperator<String> wordStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String vin) throws Exception {
                return Tuple2.of(vin, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);

        SingleOutputStreamOperator<Map<String, String>> hbaseResult = sum.map(new MapFunction<Tuple2<String, Integer>, Map<String, String>>() {
            @Override
            public Map<String, String> map(Tuple2<String, Integer> tuple2) throws Exception {
                String word = tuple2.f0;
                Integer count = tuple2.f1;

                Map<String, String> map = new HashMap<>();
                map.put(HBaseConstant.TABLE_NAME, "word");
                map.put(HBaseConstant.COLUMN_FAMILY, "cf");

                List<String> list = new ArrayList<>();
                list.add("word");
                list.add("count");
                String columnNames = Joiner.on(",").join(list);

                String rowKey = LocalDate.now() + word;
                map.put(HBaseConstant.ROW_KEY, rowKey);
                map.put(HBaseConstant.COLUMN_NAME_LIST, columnNames);
                map.put("word", word);
                map.put("count", String.valueOf(count));

                return map;
            }
        });

        hbaseResult.addSink(new HBaseSink());
    }
}
