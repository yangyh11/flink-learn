package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @description: timeWindow算子
 * @author: yangyh
 * @create: 2020-01-10 21:11
 * timeWindow是是根据时间对数据流进行分组。支持滑动窗口和滚动窗口。作用再keyBy之后
 */
public class Demo10TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("node4", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dss.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }

            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = flatMap.keyBy(0);

        // timeWindow滚动窗口,统计过去10s的数据
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow1 = keyBy.timeWindow(Time.seconds(10));

        // timeWindow滑动窗口，每隔5秒，统计过去10秒的数据
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow2 = keyBy.timeWindow(Time.seconds(10), Time.seconds(5));

        timeWindow2.sum(1).print();

        env.execute();
    }
}
