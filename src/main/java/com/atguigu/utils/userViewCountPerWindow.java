package com.atguigu.utils;

import java.sql.Timestamp;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2821:32
 */
public class userViewCountPerWindow {
    public String username;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public userViewCountPerWindow() {
    }

    public userViewCountPerWindow(String username, Long count, Long windowStartTime, Long windowEndTime) {
        this.username = username;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "(" +
                " " + username + '\'' +
                ", " + count +
                ", " + new Timestamp(windowStartTime) +
                "~" + new Timestamp(windowEndTime) +
                ')';
    }
}
