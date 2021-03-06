package com.atguigu.day09.exer;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/107:24
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("user-1", "fail", 1000L),
                        new Event("user-1", "fail", 2000L),
                        new Event("user-2", "success", 3000L),
                        new Event("user-1", "fail", 4000L),
                        new Event("user-1", "fail", 5000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.ts;
                            }
                        }));
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("login-fail")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.value.contains("fail");
                    }
                })
                .times(3)
                .consecutive();

        CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        Event first = map.get("login-fail").get(0);
                        Event second = map.get("login-fail").get(1);
                        Event third = map.get("login-fail").get(2);
                        return first.key + "???????????????" + first.ts + "," + second.ts + "," + third.ts + "????????????????????????";
                    }
                })
                .print()
        ;


        env.execute();

    }
}
