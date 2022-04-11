package com.yangyh.flink.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.time.Duration;

/**
 * Description:flink数据源读取文本连续（流）数据流
 * date: 2022/3/4 10:36 上午
 *
 * @author yangyh
 */
public class ContinuousReadTextFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String filePath = "/usr/local/yangyh/04-code/learn/flink-learn/data/state/taobao/chat.csv";

//        TextInputFormat format = new TextInputFormat(new Path(filePath));
//        DataStreamSource<String> inputStream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000).setParallelism(1);
//        inputStream.print().setParallelism(1);

        Path path = new Path(filePath);
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineFormat(), path)
                .monitorContinuously(Duration.ofSeconds(1))
                .build();
        DataStreamSource<String> streamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");

        streamSource.print();

        env.execute("流式读取文本文件");
    }
}
