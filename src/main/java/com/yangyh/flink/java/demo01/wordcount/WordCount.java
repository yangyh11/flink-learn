package com.yangyh.flink.java.demo01.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description: Flink 单词统计
 * @author: yangyh
 * @create: 2020-01-08 15:04
 * 1.Flink读取数据时需要先创建Flink的环境：
 *   批数据：
 *      ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); // 自动识别环境
 *      ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(); // 返回本地环境
 *   流数据：
 *      StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(); // 自动识别环境
 *      StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(); // 返回本地环境
 * 2.Spark&Flink
 *      1.Spark本地执行代码有WEBUI，Flink本地没有，集群有。
 *      2.Spark是面向kv格式数据的编程，Flink不是基于KV格式数据的编程。
 * 3.Flink的数据分为有界数据和无界数据
 *      批处理的数据是有界的，底层对象是DataSet，
 *      流处理的数据可以是有界的也可以是无界的，底层操作对象是DataStream。
 * 4.Flink中的Tuple最多支持25位，Scala中的Tuple最多支持22位。
 * 5.Flink代码流程：
 *      1.创建Flink的环境 ExecutionEnvironment.getExecutionEnvironment()。
 *      2.Source -> Transformations -> Sink。
 *      3.env.execute()触发以上流程执行。【print、count、collect自带触发功能，不需要env.execute()】
 * 6.Flink批数据指定虚拟的key，使用groupBy，Flink流式处理中指定虚拟的key使用keyBy
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/words");

        FlatMapOperator<String, String> words = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    out.collect(word);
                }
            }
        });

        MapOperator<String, Tuple2<String, Integer>> map = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> groupBy = map.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);

        sum.print();

        System.out.println("================");

        // 对结果排序,倒叙
        SortPartitionOperator<Tuple2<String, Integer>> result = sum.sortPartition(1, Order.DESCENDING).setParallelism(1);
        result.print();

        // 将结果写入文件
//        result.writeAsCsv("./data/result", WriteMode.OVERWRITE);
        result.writeAsCsv("./data/result", "#", "=", WriteMode.OVERWRITE);
        env.execute();
    }
}
