package com.yangyh.flink.java.demo03.virtualkey;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.util.Collector;

/**
 * @description: 使用类中指定的字段
 * @author: yangyh
 * @create: 2020-01-09 19:43
 */
public class Demo02FieldExpression {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lines = env.readTextFile("./data/words");

        FlatMapOperator<String, String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });

        MapOperator<String, Demo02MyInfo> Demo02MyInfos = words.map(new MapFunction<String, Demo02MyInfo>() {
            @Override
            public Demo02MyInfo map(String word) throws Exception {
                return new Demo02MyInfo(word, 1);
            }
        });

        // 使用Demo02MyInfos类中的word作为虚拟key
        UnsortedGrouping<Demo02MyInfo> groupBy = Demo02MyInfos.groupBy("word");

        ReduceOperator<Demo02MyInfo> reduce = groupBy.reduce(new ReduceFunction<Demo02MyInfo>() {
            @Override
            public Demo02MyInfo reduce(Demo02MyInfo value1, Demo02MyInfo value2) throws Exception {
                return new Demo02MyInfo(value1.getWord(), value1.getCount() + value2.getCount());
            }
        });

        reduce.print();
    }
}
