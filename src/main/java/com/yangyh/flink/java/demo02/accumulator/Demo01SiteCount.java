package com.yangyh.flink.java.demo02.accumulator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * @description: 计数
 * @author: yangyh
 * @create: 2020-01-08 21:48
 */
public class Demo01SiteCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/360index");

        FilterOperator<String> filter = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("https://");
            }
        });

        long count = filter.count();
        System.out.println("count=" + count);

    }
}
