package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:32
 */

public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 10*1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.ts;
                            }
                        }));

        SingleOutputStreamOperator<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right", 1000L),
                        new Event("key-1", "right", 6000L),
                        new Event("key-1", "right", 11 * 1000L),
                        new Event("key-1", "right", 16 * 1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.ts;
                            }
                        }));
        leftStream
                .keyBy(r -> r.key)
                .intervalJoin(rightStream.keyBy(r -> r.key))
                .between(Time.seconds(-5),Time.seconds(5))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event event, Event event2, ProcessJoinFunction<Event, Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(event + " => " + event2);
                    }
                })
                .print()
                ;

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
