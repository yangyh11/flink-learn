package com.yangyh.flink.java.demo05.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: socket source
 * @author: yangyh
 * @create: 2020-01-09 19:56
 */
public class Demo01Socket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamContextEnvironment.getExecutionEnvironment();

        // 读取socket数据
        DataStreamSource<String> streamSource = streamEnv.socketTextStream("node4", 9999);

        // 逻辑处理...

        streamSource.print();

        streamEnv.execute();


    }
}
