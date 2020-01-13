package com.yangyh.flink.java.demo05.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;


/**
 * @description: 读取给定的数据对象创建数据流
 * @author: yangyh
 * @create: 2020-01-09 20:22
 */
public class Demo04Elements {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取给定的数据对象创建数据流
        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d", "e");

        // 逻辑处理...

        dataSource.print();
    }
}
