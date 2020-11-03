package com.yangyh.flink.java.sample.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: yangyh
 * @create: 2020-01-08 21:36
 * 使用对象方式封装数据：
 * 1.类的访问级别必须是public。
 * 2.类中必须又无参构造。
 * 3.类中的属性必须又getter，setter方法。
 * 4.类必须是可序列化的。
 */
public class WordCount2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lines = env.readTextFile("./data/words");


        FlatMapOperator<String, MyInfo> myInfoFlatMapOperator = lines.flatMap(new FlatMapFunction<String, MyInfo>() {
            @Override
            public void flatMap(String line, Collector<MyInfo> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new MyInfo(word, 1));
                }

            }
        });

        UnsortedGrouping<MyInfo> myInfoGroup = myInfoFlatMapOperator.groupBy("word");

        ReduceOperator<MyInfo> reduce = myInfoGroup.reduce(new ReduceFunction<MyInfo>() {
            @Override
            public MyInfo reduce(MyInfo value1, MyInfo value2) throws Exception {
                return new MyInfo(value1.getWord(), value1.getCount() + value2.getCount());
            }
        });

        reduce.print();
    }
}
