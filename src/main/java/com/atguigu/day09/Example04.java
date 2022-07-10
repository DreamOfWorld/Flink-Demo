package com.atguigu.day09;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

//超时订单检测
public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                               @Override
                               public void run(SourceContext<Event> sourceContext) throws Exception {
                                   sourceContext.collectWithTimestamp(new Event("order-1", "create", 1000L), 1000L);
                                   sourceContext.collectWithTimestamp(new Event("order-2", "create", 2000L), 2000L);
                                   sourceContext.collectWithTimestamp(new Event("order-1", "pay", 3000L), 3000L);
                               }

                               @Override
                               public void cancel() {

                               }
                           });
        stream
                .keyBy(r -> r.key)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<Event> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Event>(
                                "state",
                                Types.POJO(Event.class)
                        ));
                    }

                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        if (event.value.contains("create")){
                            state.update(event);
                            context.timerService().registerEventTimeTimer(event.ts + 5000L);
                        }else if (event.value.contains("key")){
                            collector.collect(event.key + "正常支付。");
                            state.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                      if (state.value() != null && state.value().value.equals("create")){
                          out.collect(state.value().key + "超时未支付。");
                      }
                    }
                })
                .print();
        env.execute();
    }
}