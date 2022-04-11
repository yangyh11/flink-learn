package com.yangyh.flink.state.enity;

import lombok.Data;

/**
 * Description:
 * date: 2022/3/9 8:10 下午
 *
 * @author yangyh
 */
@Data
public class BehaviorPatternEntity {
    private String firstBehavior;
    private String secondBehavior;

    public BehaviorPatternEntity() {}

    public BehaviorPatternEntity(String firstBehavior, String secondBehavior) {
        this.firstBehavior = firstBehavior;
        this.secondBehavior = secondBehavior;
    }
}
