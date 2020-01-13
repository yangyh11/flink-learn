package com.yangyh.flink.java.demo02.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * @description: Flink累加器
 * @author: yangyh
 * @create: 2020-01-08 21:54
 * Flink累加器：
 *  1.需要在算子内部创建累加器对象。
 *  2.通常在Rich函数中的open方法中注册累加器，指定累加器的名称。
 *  3.在当前算子内任意位置可以使用累加器。
 *  4.必须在任务执行结束后，通过env.execute(xxx)执行后的JobExecutionResult对象获取累加器的值。
 */
public class Demo02FlinkAccumulator {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/360index");

        FilterOperator<String> filter = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("https://");
            }
        });

        MapOperator<String, String> map = filter.map(new RichMapFunction<String, String>() {
            private IntCounter counter = new IntCounter();

            // 注册累加器
            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("acc", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        DataSink<String> sink = map.writeAsText("./data/result", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult execute = env.execute();
        Integer acc = execute.getAccumulatorResult("acc");
        System.out.println("accumulator value = "+acc);

    }
}
