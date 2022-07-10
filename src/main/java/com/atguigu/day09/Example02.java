package com.atguigu.day09;

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

//连续登陆三次失败的检测
public class Example02 {
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
                //flink cep 必须使用事件时间
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.ts;
                                    }
                                })
                );
        //定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("login-fail")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.value.equals("fail");
                    }
                })
                //出现三次
                .times(3)
                //要求时间连续出现
                .consecutive();
        //在事件流上匹配模板
        //然后输出匹配到的事件组
        /**
         * @Description
         * @apiNote Map<String, List<Event>> map
         * map
         * {
         *      "login-fail":[Event,Event,Event]
         * }
         */

        CEP
                .pattern(stream.keyBy(r -> r.key),pattern)
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        Event first = map.get("login-fail").get(0);
                        Event second = map.get("login-fail").get(1);
                        Event third = map.get("login-fail").get(2);
                        return first.key + "在时间戳：" + first.ts + "," + second.ts + "," + third.ts + "连续三次登录失败";
                    }
                })
                .print();
        env.execute();
    }
}