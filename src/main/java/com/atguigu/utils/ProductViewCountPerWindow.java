package com.atguigu.utils;

import java.sql.Timestamp;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/414:47
 */
public class ProductViewCountPerWindow {
    public String productId;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public ProductViewCountPerWindow() {
    }

    public ProductViewCountPerWindow(String productId, Long count, Long windowStartTime, Long windowEndTime) {
        this.productId = productId;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "(" +
                "商品Id='" + productId +
                ", 浏览次数=" + count +
                ", " + new Timestamp(windowStartTime) +
                "~" + new Timestamp(windowEndTime) +
                ')';
    }
}
