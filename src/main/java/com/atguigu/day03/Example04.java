package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        data
                .map(r -> Tuple2.of(r,1))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
                        return Tuple2.of(integerIntegerTuple2.f0 + t1.f0,integerIntegerTuple2.f1 + t1.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Integer, Integer>, String>() {
                    @Override
                    public String map(Tuple2<Integer, Integer> value) throws Exception {
                        return "平均值是" + value.f0 / value.f1;
                    }
                })
                .print();

        env.execute();


    }
}
