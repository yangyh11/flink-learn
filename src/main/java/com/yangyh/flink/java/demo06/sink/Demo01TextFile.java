package com.yangyh.flink.java.demo06.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * @description: Kafka将数据写入文本文件
 * @author: yangyh
 * @create: 2020-01-09 21:11
 */
public class Demo01TextFile {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d", "e");

        // 逻辑处理...

        // Kafka将数据写入文本文件
        dataSource.writeAsText("./data/result", WriteMode.OVERWRITE);

        env.execute();
    }
}
