package com.yangyh.flink.java.demo04.func;

import com.yangyh.flink.java.demo05.source.Demo05NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: partitionCustom 算子
 * @author: yangyh
 * @create: 2020-01-11 11:17
 * partitionCustom可以对Flink的流式数据指定分区器进行重新分区。
 * 案例：
 *  使用自定义source作为数据源，使用自定义分区器定义分区规则。根据数据奇偶分区
 */
public class Demo14PartitionCustom {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> dds = env.addSource(new Demo05NoParallelSource());

        SingleOutputStreamOperator<Tuple1<Integer>> map = dds.map(new MapFunction<Integer, Tuple1<Integer>>() {
            @Override
            public Tuple1<Integer> map(Integer value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        // partitionCustom算子重新分区,将做Tuple1的第一位作为key，进行分组
        DataStream<Tuple1<Integer>> partitionCustom = map.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 2;
            }
        }, 0);

        SingleOutputStreamOperator<Integer> map1 = partitionCustom.map(new MapFunction<Tuple1<Integer>, Integer>() {
            @Override
            public Integer map(Tuple1<Integer> value) throws Exception {
                return value.f0;
            }
        });

        map1.print();

        env.execute();

    }
}
