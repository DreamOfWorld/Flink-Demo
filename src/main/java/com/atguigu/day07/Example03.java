package com.atguigu.day07;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:32
 */
//实时对账
    //如果left事件先到达，那么等待right事件5秒钟，如果没有等到，输出对账失败
    //如果right事件先到达，那么等待left事件5秒钟，如果没有等到，输出对账失败
public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(
                                new Event("key-1", "left", 1000L),
                                1000L
                        );
                        ctx.collectWithTimestamp(
                                new Event("key-2", "left", 2000L),
                                2000L
                        );

                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(
                                new Event("key-1", "right", 4000L),
                                4000L
                        );
                        ctx.collectWithTimestamp(
                                new Event("key-3", "right", 8000L),
                                8000L
                        );
                    }

                    @Override
                    public void cancel() {

                    }
                });

        leftStream.keyBy(r -> r.key)
                .connect(rightStream.keyBy(r -> r.key))
                .process(new CoProcessFunction<Event, Event, String>() {
                    private ValueState<Event> leftState;
                    private ValueState<Event> rightState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        leftState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Event>(
                                        "left-state",
                                        Types.POJO(Event.class)
                                )
                        );
                        rightState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Event>(
                                        "right-state",
                                        Types.POJO(Event.class)
                                )
                        );
                    }

                    @Override
                    public void processElement1(Event in1, Context ctx, Collector<String> out) throws Exception {
                        // left事件先到达
                        if (rightState.value() == null) {
                            // 将left事件保存下来
                            leftState.update(in1);
                            // 注册5秒钟之后的定时器
                            ctx.timerService().registerEventTimeTimer(
                                    in1.ts + 5000L
                            );
                        }
                        // right事件先到达
                        else {
                            out.collect(in1.key + "对账成功，right事件先到达。");
                            // 将rightState清空
                            rightState.clear();
                        }
                    }

                    @Override
                    public void processElement2(Event in2, Context ctx, Collector<String> out) throws Exception {
                        // 和processElement1的实现完全对称
                        if (leftState.value() == null) {
                            rightState.update(in2);
                            ctx.timerService().registerEventTimeTimer(
                                    in2.ts + 5000L
                            );
                        }
                        else {
                            out.collect(in2.key + "对账成功，left事件先到达。");
                            leftState.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timerTs, OnTimerContext ctx, Collector<String> out) throws Exception {
                        if (leftState.value() != null) {
                            out.collect(leftState.value().key + "对账失败，right事件没来。");
                            leftState.clear();
                        }
                        if (rightState.value() != null) {
                            out.collect(rightState.value().key + "对账失败，left事件没来。");
                            rightState.clear();
                        }
                    }
                })
                .print();

        env.execute();
    }


    public static class Event{
        public String key;
        public String value;
        public Long ts;

        public Event() {
        }

        public Event(String key, String value, Long ts) {
            this.key = key;
            this.value = value;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "(" +
                    key +
                    "," + value +
                    "," + ts +
                    ')';
        }
    }

}
