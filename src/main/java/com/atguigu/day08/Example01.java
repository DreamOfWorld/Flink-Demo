package com.atguigu.day08;

import com.atguigu.utils.userBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.mutable.HashSet;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:32
 */
public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .readTextFile("D:\\project\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, userBehavior>() {
                    @Override
                    public userBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new userBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<userBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<userBehavior>() {
                                    @Override
                                    public long extractTimestamp(userBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> "user-behavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<userBehavior, HashSet<String>, Long>() {
                            @Override
                            public HashSet<String> createAccumulator() {

                                return new HashSet<>();
                            }

                            @Override
                            public HashSet<String> add(userBehavior userBehavior, HashSet<String> accumulator) {
                                // 如果in.userId在hashset中已经存在，那么add将不起作用
                                // 每个in.userId只会在hashset中添加一次，幂等性
                                accumulator.add(userBehavior.userId);
                                return accumulator;
                            }

                            @Override
                            public Long getResult(HashSet<String> accumulator) {
                                // 每个窗口中的独立访客数量就是hashset中的元素数量
                                return (long)accumulator.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> stringHashSet, HashSet<String> acc1) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, ProcessWindowFunction<Long, String, String, TimeWindow>.Context ctx, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口：" + new Timestamp(ctx.window().getStart()) + "~" +
                                        "" + new Timestamp(ctx.window().getEnd()) + "的uv是：" +
                                        "" + elements.iterator().next());
                            }
                        }
                )
                .print();
        env.execute();
    }


}
