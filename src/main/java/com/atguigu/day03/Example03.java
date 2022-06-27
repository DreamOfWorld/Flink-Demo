package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        data
                .map(new MapFunction<Integer, Tuple3<Integer,Integer,Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> map(Integer value) throws Exception {
                        return Tuple3.of(value,value,value);
                    }
                })
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> in, Tuple3<Integer, Integer, Integer> acc) throws Exception {
                        return Tuple3.of(
                                Math.max(in.f0,acc.f0),
                                Math.min(in.f1,acc.f1),
                                in.f2 + acc.f2
                        );
                    }
                })
                .print();

        env.execute();


    }
}
