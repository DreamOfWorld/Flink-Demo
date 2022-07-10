package com.atguigu.day07.exer;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:46
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> addSource = env.addSource(new ClickSource());
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
        addSource
                .keyBy(r -> r.key)
                .connect(socketTextStream.setParallelism(1).broadcast())
                .flatMap(new CoFlatMapFunction<Event, String, Event>() {
                    private String socket;
                    @Override
                    public void flatMap1(Event event, Collector<Event> collector) throws Exception {
                        if (event.key.equals(socket)){
                            collector.collect(event);
                        }
                    }

                    @Override
                    public void flatMap2(String s, Collector<Event> collector) throws Exception {
                        socket = s;
                    }
                })
                .print();

        env.execute();
    }
}
