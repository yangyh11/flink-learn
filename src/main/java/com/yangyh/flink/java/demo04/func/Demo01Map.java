package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * @description: map算子
 * @author: yangyh
 * @create: 2020-01-09 21:35
 * map算子一对一消费数据，DataStream -> DataStream
 */
public class Demo01Map {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/words");

        // map算子
        MapOperator<String, String> map = dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "#";
            }
        });

        map.print();

    }
}
