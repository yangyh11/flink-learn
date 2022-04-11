package com.yangyh.flink.state.function;

import com.yangyh.flink.state.enity.BehaviorPatternEntity;
import com.yangyh.flink.state.enity.UserBehaviorEntity;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Description:
 * date: 2022/3/9 8:34 下午
 *
 * @author yangyh
 */
public class BroadcastPatternFunction extends KeyedBroadcastProcessFunction<Long, UserBehaviorEntity, BehaviorPatternEntity, Tuple2<Long, BehaviorPatternEntity>> {
    // 用户上次行为的状态句柄，每个用户存储一个状态
    private ValueState<String> lastBehaviorState;
    private MapStateDescriptor<Void, BehaviorPatternEntity> bcPatternDesc;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastBehaviorState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastBehavior", Types.STRING));
        bcPatternDesc = new MapStateDescriptor<Void, BehaviorPatternEntity>("behaviorPattern",
                Types.VOID, Types.POJO(BehaviorPatternEntity.class));
    }

    @Override
    public void processElement(UserBehaviorEntity userBehaviorEntity, ReadOnlyContext readOnlyContext, Collector<Tuple2<Long, BehaviorPatternEntity>> collector) throws Exception {
        // 获取最新的广播状态
        ReadOnlyBroadcastState<Void, BehaviorPatternEntity> readOnlyBcState = readOnlyContext.getBroadcastState(bcPatternDesc);
        BehaviorPatternEntity patternEntity = readOnlyBcState.get(null);
        String lastBehavior = lastBehaviorState.value();
        if (patternEntity != null && lastBehavior != null) {
            // 用户之前有过行为，检查是否符合给定的模式
            if (patternEntity.getFirstBehavior().equals(lastBehavior) &&
                patternEntity.getSecondBehavior().equals(userBehaviorEntity.getBehavior())) {
                // 当前用户行为符合模式
                collector.collect(Tuple2.of(userBehaviorEntity.getUserId(), patternEntity));
            }
        }
        lastBehaviorState.update(userBehaviorEntity.getBehavior());
    }

    @Override
    public void processBroadcastElement(BehaviorPatternEntity patternEntity, Context context, Collector<Tuple2<Long, BehaviorPatternEntity>> collector) throws Exception {
        BroadcastState<Void, BehaviorPatternEntity> bcPatternState = context.getBroadcastState(bcPatternDesc);
        // 将新数据更新至BroadcastState，这里使用null作为key
        // 在本场景中，所有数据都共享一个模式，因此这里伪造一个key
        bcPatternState.put(null, patternEntity);
    }
}
