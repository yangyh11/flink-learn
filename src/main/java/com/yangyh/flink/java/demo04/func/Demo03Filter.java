package com.yangyh.flink.java.demo04.func;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * @description: filter算子，过滤数据
 * @author: yangyh
 * @create: 2020-01-09 21:51
 * filter算子，过滤数据。DataStream → DataStream。
 */
public class Demo03Filter {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("./data/words");

        // filter算子
        FilterOperator<String> filter = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return "hello flink".equals(value);
            }
        });

        filter.print();

    }
}
