package com.atguigu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description map举例
 * @Author mei
 * @Data 2022/6/2716:02
 */
//filter举例
public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.key.equals("Mary") ;
                    }
                })
                .print("使用匿名类实现filter算子");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("使用外部类实现filter算子");


        env
                .addSource(new ClickSource())
                .filter(r -> r.key.equals("Mary"))
                .print("使用匿名函数实现filter算子");
        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<Event, Event>() {

                    @Override
                    public void flatMap(Event event, Collector<Event> collector) throws Exception {
                        if (event.key.equals("Mary")){
                            collector.collect(event);
                    }
                }
                })
                .print("使用flatMap实现filter算子");
        env.execute();

    }
    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return event.key.equals("Mary") ;
        }
    }
}
