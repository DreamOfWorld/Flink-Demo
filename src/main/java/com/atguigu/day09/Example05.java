package com.atguigu.day09;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

//连续登陆三次失败的检测
public class Example05 {
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
       stream
               .keyBy(r -> r.key)
               .process(new StateMachine())
               .print()
       ;
        env.execute();
    }
    public static class StateMachine extends KeyedProcessFunction<String,Event,String>{
        private HashMap<Tuple2<String,String>,String> stateMachine = new HashMap<>();
        private ValueState<String> currentState;
        @Override
        public void open(Configuration parameters) throws Exception {
            stateMachine.put(Tuple2.of("INITIAL","fail"),"S1");
            stateMachine.put(Tuple2.of("INITIAL","success"),"success");
            stateMachine.put(Tuple2.of("S1","fail"),"S2");
            stateMachine.put(Tuple2.of("S1","success"),"success");
            stateMachine.put(Tuple2.of("S2","fail"),"FAIL");
            stateMachine.put(Tuple2.of("S2","success"),"success");
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                    "currentState",
                    Types.STRING
            ));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            if (currentState.value() == null){
                //到达的是第一条数据
                currentState.update("INITIAL");
            }
            //计算将要跳转到的数据
            String nextState = stateMachine.get(Tuple2.of(currentState.value(), event.value));
            if (nextState.equals("FAIL")){
                collector.collect("连续登录3次失败。");
                //重置到S2状态
                currentState.update("S2");
            } else if (nextState.equals("SUCCESS")) {
                currentState.update("INITIAL");
            }else {
                currentState.update(nextState);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }
}