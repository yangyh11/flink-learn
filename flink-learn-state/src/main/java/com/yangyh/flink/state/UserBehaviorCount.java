package com.yangyh.flink.state;

import com.yangyh.flink.state.enity.UserBehaviorEntity;
import com.yangyh.flink.state.function.MapStateFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description: 用户行为统计：用户id分组，行为次数累加
 * date: 2022/3/3 7:35 下午
 *
 * @author yangyh
 */
public class UserBehaviorCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String filePath = "/usr/local/yangyh/04-code/learn/flink-learn/data/state/taobao/UserBehavior-20171201.csv";
        Path path = new Path(filePath);
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineFormat(), path).build();
        DataStreamSource<String> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        DataStream<UserBehaviorEntity> userBehaviorStream = inputStream.map(elem -> {
            String[] cells = elem.split(",");
            return new UserBehaviorEntity(Long.parseLong(cells[0]), Long.parseLong(cells[1]), Integer.parseInt(cells[2]), cells[3], Long.parseLong(cells[4]));
        });

        // 每个用户的行为数据共享自己的状态数据
        KeyedStream<UserBehaviorEntity, Long> keyedStream = userBehaviorStream.keyBy(userBehaviorEntity -> userBehaviorEntity.getUserId());

        DataStream<Tuple3<Long, String, Integer>> behaviorCountStream = keyedStream.flatMap(new MapStateFunction());

        behaviorCountStream.print();

        env.execute("用户行为统计");

    }
}
