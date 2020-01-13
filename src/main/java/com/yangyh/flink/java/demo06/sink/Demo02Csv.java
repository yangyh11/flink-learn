package com.yangyh.flink.java.demo06.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/**
 * @description: Kafka将数据写入CSV文件
 * @author: yangyh
 * @create: 2020-01-09 21:15
 * The writeAsCsv() method can only be used on data sets of tuples
 */
public class Demo02Csv {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d", "e");

        FlatMapOperator<String, String> flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        });

        MapOperator<String, Tuple1<String>> map = flatMap.map(new MapFunction<String, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(String value) throws Exception {
                return new Tuple1(value);
            }
        });

        // Kafka将数据写入文本文件
        map.writeAsCsv("./data/result", WriteMode.OVERWRITE);

        env.execute();
    }

}
