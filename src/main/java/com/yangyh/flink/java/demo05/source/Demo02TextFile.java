package com.yangyh.flink.java.demo05.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @description: 读取文件
 * @author: yangyh
 * @create: 2020-01-09 20:10
 */
public class Demo02TextFile {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        DataSource<String> dataSource = env.readTextFile("./data/words");

        // 逻辑处理...

        dataSource.print();

    }
}
