package com.atguigu.Exercise;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.userViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2822:42
 */
public class demo09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new WindowResult())
                .print();
        env.execute();
    }
    public static class WindowResult extends ProcessWindowFunction<Event, userViewCountPerWindow,String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Event, userViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Event> iterable, Collector<userViewCountPerWindow> collector) throws Exception {
            collector.collect(new userViewCountPerWindow(
                    s,
                    iterable.spliterator().getExactSizeIfKnown(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }
}
