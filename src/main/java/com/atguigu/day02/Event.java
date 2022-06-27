package com.atguigu.day02;

import java.sql.Timestamp;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2715:58
 */
//必须是共有类
public class Event {
    //所有字段必须是public
    public String key;
    public String value;
    public Long ts;
    //空构造器
    public Event() {
    }

    public Event(String key, String value, Long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
