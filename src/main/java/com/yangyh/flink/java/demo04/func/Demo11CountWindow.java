package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @description: countWindow 算子
 * @author: yangyh
 * @create: 2020-01-10 21:28
 * 根据个数对数据流进行窗口划分，支持滚动窗口和滑动窗口。在keyBy之后使用
 */
public class Demo11CountWindow {

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

        // countWindow,滚动窗口，统计过去10条数据
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow1 = keyBy.countWindow(10);

        // countWindow,滑动窗口，每隔5条数据，统计过去10条数据
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow2 = keyBy.countWindow(10, 5);

        countWindow2.sum(1).print();

        env.execute();
    }
}
