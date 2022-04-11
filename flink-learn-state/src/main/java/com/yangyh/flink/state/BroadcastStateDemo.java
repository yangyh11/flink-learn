package com.yangyh.flink.state;

import com.yangyh.flink.state.enity.BehaviorPatternEntity;
import com.yangyh.flink.state.enity.UserBehaviorEntity;
import com.yangyh.flink.state.function.BroadcastPatternFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Description: 广播变量用例
 * date: 2022/3/9 8:12 下午
 *
 * @author yangyh
 */
public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String filePath = "/usr/local/yangyh/04-code/learn/flink-learn/data/state/taobao/UserBehavior-20171201.csv";
        Path path = new Path(filePath);
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineFormat(), path).build();
        // 主数据流
        DataStreamSource<String> inputStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        DataStream<UserBehaviorEntity> userBehaviorStream = inputStream.map(elem -> {
            String[] cells = elem.split(",");
            return new UserBehaviorEntity(Long.parseLong(cells[0]), Long.parseLong(cells[1]), Integer.parseInt(cells[2]), cells[3], Long.parseLong(cells[4]));
        });

        // BehaviorPattern数据流
        DataStream<BehaviorPatternEntity> patternStream = env.fromElements(new BehaviorPatternEntity("pv", "buy"));

        // 将监控规则广播出去
        MapStateDescriptor<Void, BehaviorPatternEntity> broadcastStateDescriptor = new MapStateDescriptor<>("behaviorPattern", Types.VOID, Types.POJO(BehaviorPatternEntity.class));
        BroadcastStream<BehaviorPatternEntity> broadcastStream = patternStream.broadcast(broadcastStateDescriptor);

        // 用户行为数据流先按照用户ID进行keyBy，然后与广播流合并
        KeyedStream<UserBehaviorEntity, Long> keyedStream = userBehaviorStream.keyBy(user -> user.getUserId());

        // 在keyedStream上进行connect()和process()
        BroadcastConnectedStream<UserBehaviorEntity, BehaviorPatternEntity> connect = keyedStream.connect(broadcastStream);
        DataStream<Tuple2<Long, BehaviorPatternEntity>> matchStream = connect.process(new BroadcastPatternFunction());
        matchStream.print();

        env.execute("广播变量测试用例");
    }
}
