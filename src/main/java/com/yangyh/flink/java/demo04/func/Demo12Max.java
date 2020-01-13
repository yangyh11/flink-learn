package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @description: max & maxBy算子
 * @author: yangyh
 * @create: 2020-01-10 21:44
 * max获取窗口范围内当前字段的最大值，
 * maxBy获取窗口范围内当前字段最大值对对应的这条数据。
 * 输入：
 *    1,zhangsan,m,50
 *    2,lisi,m,100
 *    3,wangwu,m,90
 *    4,zhaoliu,m,10
 *    5,tianqi,m,20
 */
public class Demo12Max {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.socketTextStream("node4", 9999);

        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> map = dss.map(new MapFunction<String, Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple4<>(split[0], split[1], split[2], Integer.valueOf(split[3]));
            }
        });

        // 将所有数据都分到一个组
        KeyedStream<Tuple4<String, String, String, Integer>, Integer> keyBy = map.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple4<String, String, String, Integer> value) throws Exception {
                return 0;
            }
        });

        WindowedStream<Tuple4<String, String, String, Integer>, Integer, TimeWindow> timeWindow = keyBy.timeWindow(Time.seconds(10));

        // Tuple4中第四位，只能保证返回的Tuple4的第四位是最大的
        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> max = timeWindow.max(3);

        SingleOutputStreamOperator<Integer> map1 = max.map(new MapFunction<Tuple4<String, String, String, Integer>, Integer>() {
            @Override
            public Integer map(Tuple4<String, String, String, Integer> value) throws Exception {
                return value.f3;
            }
        });

        // 返回Tuple4中第四位最大的那一条数据
        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> maxBy = timeWindow.maxBy(3);

        max.print();
        map1.print();
        maxBy.print();

        env.execute();

    }
}
