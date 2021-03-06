package com.atguigu.day07;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.ProductViewCountPerWindow;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;



/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:32
 */

public class Example08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.ts;
                            }
                        }))
                .keyBy(r ->r.key)
                .window(TumblingEventTimeWindows.of(Time.days(1)))

                .trigger(new Trigger<Event, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Event event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        //????????????????????????????????????
                        // ???????????????????????????????????????????????????

                        //?????????????????????????????????????????????
                        ValueState<Boolean> flag = triggerContext.getPartitionedState(
                                new ValueStateDescriptor<Boolean>(
                                        "flag",
                                        Types.BOOLEAN
                                )
                        );

                        //????????????????????????????????????????????????
                        if (flag.value() == null){
                            //???????????????????????????????????????
                            //1234ms + 1000ms - 1234 % 1000 -> 2000ms
                            long nextSecond = event.ts + 1000L - event.ts  % 1000L;
                            //???????????????????????????onEventTime??????
                            triggerContext.registerEventTimeTimer(nextSecond);
                            //??????????????????true
                            flag.update(true);
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long timerTs, TimeWindow window, TriggerContext triggerContext) throws Exception {
                        if (timerTs < window.getEnd()){
                            if (timerTs + 1000L < window.getEnd()){
                                //??????????????????????????????onEventTime??????
                                triggerContext.registerEventTimeTimer(timerTs + 1000L);
                            }
                            //?????? ????????????
                            return TriggerResult.FIRE;
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        ValueState<Boolean> flag = triggerContext.getPartitionedState(
                                new ValueStateDescriptor<Boolean>(
                                        "flag",
                                        Types.BOOLEAN
                                )
                        );
                        //????????????????????????????????????
                        flag.clear();

                    }
                })
                .aggregate(
                        new AggregateFunction<Event, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Event value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<Long> iterable, Collector<ProductViewCountPerWindow> out) throws Exception {
                                out.collect(new ProductViewCountPerWindow(
                                        key,
                                        iterable.iterator().next(),
                                        ctx.window().getStart(),
                                        ctx.window().getEnd()
                                ));
                            }
                        }
                )
                .print();

        env.execute();
    }



}
