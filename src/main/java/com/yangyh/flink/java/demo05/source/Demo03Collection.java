package com.yangyh.flink.java.demo05.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

/**
 * @description: 读取集合的数据
 * @author: yangyh
 * @create: 2020-01-09 20:15
 */
public class Demo03Collection {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取集合里的数据
        DataSource<String> dataSource = env.fromCollection(Arrays.asList("a", "b", "c", "d"));

        // 逻辑处理...

        dataSource.print();
    }
}
