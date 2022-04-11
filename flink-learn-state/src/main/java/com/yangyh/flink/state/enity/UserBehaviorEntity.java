package com.yangyh.flink.state.enity;

import lombok.Data;

/**
 * Description:
 * date: 2022/3/4 3:16 下午
 *
 * @author yangyh
 */
@Data
public class UserBehaviorEntity {
    private long userId;
    private long itemId;
    private int categoryId;
    private String behavior;
    private long timestamp;

    public UserBehaviorEntity() {
    }

    public UserBehaviorEntity(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
