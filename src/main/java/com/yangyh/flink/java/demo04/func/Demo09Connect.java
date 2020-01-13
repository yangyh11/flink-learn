package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * @description: connect算子
 * @author: yangyh
 * @create: 2020-01-09 22:05
 * connect算子连接两个流，连接的两个流可以是不同类型的流。
 * 如果连接两个类型相同的流与union类似，connect会对两个流中的数据应用不同方法处理。
 * DataStream,DataStream –> ConnectedStreams。
 */
public class Demo09Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss1 = env.fromCollection(Arrays.asList("a", "b", "c"));
        DataStreamSource<Integer> dss2 = env.fromElements(1, 2, 3);

        // connect算子
        ConnectedStreams<String, Integer> connect = dss1.connect(dss2);

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String str) throws Exception {
                return str;
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return integer.toString();
            }
        });

        map.print();

        env.execute();

    }
}
