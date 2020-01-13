package com.yangyh.flink.java.demo03.virtualkey;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @description: 使用key Selector这种方式选择key
 * @author: yangyh
 * @create: 2020-01-09 19:47
 */
public class Demo03Selector {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/virtualKey.txt");

        // 数据结构：杨 18 男
        // 将性别作为虚拟的key
        UnsortedGrouping<String> groupBy = dataSource.groupBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String line) throws Exception {
                return line.split(" ")[2];
            }
        });

        ReduceOperator<String> reduce = groupBy.reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {
                return value1 + "#" + value2;
            }
        });

        reduce.print();

    }
}
