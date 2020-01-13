package com.yangyh.flink.java.demo03.virtualkey;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @description: 使用Tuple来指定key
 * @author: yangyh
 * @create: 2020-01-09 19:17
 */
public class Demo01Tuple {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataSource<String> dataSource = env.readTextFile("./data/virtualKey.txt");

        // 数据格式：李 16 男
        FlatMapOperator<String, Tuple3<String, Integer, String>> flatMap = dataSource.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                String[] strings = value.split(" ");
                Tuple3<String, Integer, String> tp3 = new Tuple3<>(strings[0], Integer.valueOf(strings[1]), strings[2]);
                out.collect(tp3);
            }
        });

        // 将姓氏作为虚拟key
        UnsortedGrouping<Tuple3<String, Integer, String>> groupBy = flatMap.groupBy(0);
        // 将年龄作为虚拟key
//        UnsortedGrouping<Tuple3<String, Integer, String>> groupBy = flatMap.groupBy(1);
        // 将性别作为虚拟key
//        UnsortedGrouping<Tuple3<String, Integer, String>> groupBy = flatMap.groupBy(2);

        ReduceOperator<Tuple3<String, Integer, String>> reduce = groupBy.reduce(new ReduceFunction<Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> tp1, Tuple3<String, Integer, String> tp2) throws Exception {
                return new Tuple3<>(tp1.f0 + "#" + tp2.f0, tp1.f1 + tp2.f1, tp1.f2 + "#" + tp2.f2);
            }
        });

        reduce.print();

    }
}
