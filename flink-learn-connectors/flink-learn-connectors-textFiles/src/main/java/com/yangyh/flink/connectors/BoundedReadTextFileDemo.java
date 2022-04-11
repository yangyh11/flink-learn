package com.yangyh.flink.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description: flink数据源读取文本有界（批）数据流
 * date: 2022/3/3 7:43 下午
 *
 * @author yangyh
 */
public class BoundedReadTextFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = "/usr/local/yangyh/04-code/learn/flink-learn/data/state/taobao/UserBehavior-20171201.csv";
        Path path = new Path(filePath);
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineFormat(), path).build();
        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        streamSource.print();

        env.execute("读取文本文件数据");
    }
}
