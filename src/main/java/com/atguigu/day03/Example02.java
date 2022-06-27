package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        data.keyBy(r -> 2).sum(0).print("sum");
        data.keyBy(r -> 2).max(0).print("max");
        data.keyBy(r -> 2).min(0).print("min");

        env.execute();


    }
}
