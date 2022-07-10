package com.atguigu.Exercise;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2814:58
 */
public class demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .map(new MapFunction<Event, Tuple2<Tuple2<String,String>,Integer>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> map(Event event) throws Exception {
                        return Tuple2.of(
                                Tuple2.of(
                                        event.key,
                                        event.value
                                ),
                                1
                        );
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Integer> in) throws Exception {
                        return in.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<Tuple2<String, String>, Integer>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> reduce(Tuple2<Tuple2<String, String>, Integer> t1, Tuple2<Tuple2<String, String>, Integer> t2) throws Exception {
                        return Tuple2.of(
                                t1.f0,
                                t1.f1 + t2.f1
                        );
                    }
                })
                .print()
        ;
        env.execute();
    }
}
