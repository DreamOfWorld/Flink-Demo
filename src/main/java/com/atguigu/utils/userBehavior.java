package com.atguigu.utils;

import java.sql.Timestamp;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/414:38
 */
public class userBehavior {

    public String userId;
    public String productId;
    public String categoryId;
    public String type;
    public Long ts;

    public userBehavior() {
    }

    public userBehavior(String userId, String productId, String categoryId, String type, Long ts) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "userBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + new Timestamp(ts )+
                '}';
    }
}
