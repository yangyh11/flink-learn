package com.yangyh.flink.java.demo04.func;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @description: union算子
 * @author: yangyh
 * @create: 2020-01-09 21:56
 * union算子合并流，可以合并多个流，合并流的类型必须一致。DataStream*-> DataStream。
 */
public class Demo08Union {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss1 = env.fromCollection(Arrays.asList("a", "b", "c"));
        DataStreamSource<String> dss2 = env.fromElements("1", "2", "3");

        // union算子
        DataStream<String> union = dss1.union(dss2);

        union.print();

        env.execute();

    }

}
