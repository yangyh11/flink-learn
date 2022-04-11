package com.yangyh.flink.state.function;

import com.yangyh.flink.state.enity.UserBehaviorEntity;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Description:
 * date: 2022/3/7 10:48 上午
 *
 * @author yangyh
 */
public class MapStateFunction extends RichFlatMapFunction<UserBehaviorEntity, Tuple3<Long, String, Integer>> {
    private MapState<String, Integer> behaviorMapState;

    // 初始化方法
    @Override
    public void open(Configuration parameters) throws Exception {
        // behaviorMapState：状态的名字可以区分不同的状态
        // 指定状态的数据类型，状态的备份和恢复时，需要对其进行序列化和反序列化
        MapStateDescriptor<String, Integer> behaviorMapStateDescriptor = new MapStateDescriptor<>("behaviorMapState", Types.STRING, Types.INT);
        behaviorMapState = getRuntimeContext().getMapState(behaviorMapStateDescriptor);
    }

    @Override
    public void flatMap(UserBehaviorEntity input, Collector<Tuple3<Long, String, Integer>> collector) throws Exception {
        int behaviorCnt = 1;
        if (behaviorMapState.contains(input.getBehavior())) {
            behaviorCnt = behaviorMapState.get(input.getBehavior()) + 1;
        }
        // 更新状态
        behaviorMapState.put(input.getBehavior(), behaviorCnt);
        collector.collect(Tuple3.of(input.getUserId(), input.getBehavior(), behaviorCnt));
    }
}
