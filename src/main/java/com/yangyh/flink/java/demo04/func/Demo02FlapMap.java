package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

/**
 * @description: flapMap算子
 * @author: yangyh
 * @create: 2020-01-09 21:46
 * flatMap算子一对多消费数据，进来一条数据出去多条数据。DataStream -> DataStream
 */
public class Demo02FlapMap {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/words");

        // flapMap算子
        FlatMapOperator<String, String> flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }
        });

        flatMap.print();

    }
}
