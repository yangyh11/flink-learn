package com.yangyh.flink.java.demo05.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: 读取自定义Source的数据
 * @author: yangyh
 * @create: 2020-01-09 20:27
 */
public class Demo05Defined {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Integer> streamSource = streamEnv.addSource(new Demo05NoParallelSource());
//        DataStreamSource<Integer> streamSource = streamEnv.addSource(new Demo05RichNoParallelSource());
//        DataStreamSource<Integer> streamSource = streamEnv.addSource(new Demo05ParallelSource());
        DataStreamSource<Integer> streamSource = streamEnv.addSource(new Demo05RichParallelSource());

        // 数据处理...

        streamSource.print();

        streamEnv.execute();

    }
}
