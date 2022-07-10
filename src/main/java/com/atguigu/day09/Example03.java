package com.atguigu.day09;

import com.atguigu.utils.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

//超时订单检测
public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> sourceContext) throws Exception {
                        sourceContext.collectWithTimestamp(new Event("order-1","create",1000L),1000L);
                        sourceContext.collectWithTimestamp(new Event("order-2","create",2000L),2000L);
                        sourceContext.collectWithTimestamp(new Event("order-1","pay",3000L),3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });
        //定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create-order")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.value.equals("create");
                    }
                })
                .next("pay-order")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.value.contains("pay");
                    }
                })
                //模板中的两个事件在5秒钟之内发生
                .within(Time.seconds(5))
                ;
        //在事件流上匹配模板
        //然后输出匹配到的事件组
        /**
         * @apiNote flatSelect()
         * 参数1：接受订单超时信息的测输出流
         * 参数2：处理超时订单的匿名类
         * 参数3：处理正常支付的订单信息
         * @parameterNote  timeout(Map<String, List<Event>>
         * map
         * {
         *      "create-order":[Event]
         * }
         *
         * @ParameterNote select(Map<String, List<Event>> map)
         * map
         * {
         *      "create-order":[Event],
         *      "pay-order":[Event]
         * }
         */

        SingleOutputStreamOperator<String> result = CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                .flatSelect(
                        new OutputTag<String>("timeout-order") {
                        },
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long l, Collector<String> collector) throws Exception {
                                Event create = map.get("create-order").get(0);
                                collector.collect(create.key + "超时未支付");
                            }
                        },
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                                Event create = map.get("create-order").get(0);
                                Event pay = map.get("pay-order").get(0);
                                collector.collect(create.key + "在" + pay.ts + "支付。");
                            }
                        }
                );
        result.print("主流");
        result.getSideOutput(new OutputTag<String>("timeout-order"){}).print("侧输出流");
        env.execute();
    }
}