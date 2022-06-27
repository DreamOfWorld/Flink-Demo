package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2716:02
 */
public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .addSource(new ClickSource())
                .print();
        env.execute();

    }
}
