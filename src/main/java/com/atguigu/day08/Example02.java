package com.atguigu.day08;

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
 * @Data 2022/7/520:32
 */
public class Example02 {
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
                //布隆过滤器只能告诉我们一个userId一定没来过，或者可能来过
                //无法告诉我们有多少userId一定没来过
                //Tuple2<布隆过滤器，一定没来过的userId的总数的统计值>
                .aggregate(
                        new AggregateFunction<userBehavior, Tuple2<BloomFilter<String>,Long>, Long>() {
                            @Override
                            public Tuple2<BloomFilter<String>, Long> createAccumulator() {

                                return Tuple2.of(
                                        BloomFilter.create(
                                                Funnels.stringFunnel(Charsets.UTF_8),//待去重类型是字符串
                                                100000,//估算的待去重的数据量
                                                0.01//误判率
                                        ),
                                        0L//统计值的初始值
                                );
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> add(userBehavior in, Tuple2<BloomFilter<String>, Long> accumulator) {
                                //如果in.userId之前一定没来过
                                if (!accumulator.f0.mightContain(in.userId)){
                                    //相关bit位置为1
                                    accumulator.f0.put(in.userId);
                                    //统计数据+1
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
        ;





        env.execute();
    }


}
