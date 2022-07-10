package com.atguigu.day08.exer;

import com.atguigu.utils.userBehavior;
import com.google.common.base.Charsets;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/622:39
 */
public class Demo01 {
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
                .aggregate(new AggregateFunction<userBehavior, Tuple2<BloomFilter<String>,Long>, Long>() {
                               @Override
                               public Tuple2<BloomFilter<String>, Long> createAccumulator() {
                                   return Tuple2.of(
                                           BloomFilter.create(
                                                   Funnels.stringFunnel(Charsets.UTF_8),
                                                   100000,
                                                   0.01
                                           ),
                                           0L
                                   );
                               }

                               @Override
                               public Tuple2<BloomFilter<String>, Long> add(userBehavior userBehavior, Tuple2<BloomFilter<String>, Long> accumulator) {
                                   if (!accumulator.f0.mightContain(userBehavior.userId)){
                                       accumulator.f0.put(userBehavior.userId);
                                       accumulator.f1 += 1L;
                                   }
                                   return accumulator;
                               }

                               @Override
                               public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
                                   return accumulator.f1;
                               }

                               @Override
                               public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2, Tuple2<BloomFilter<String>, Long> acc1) {
                                   return null;
                               }
                           }
                        ,
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
        ;





        env.execute();
    }
}
