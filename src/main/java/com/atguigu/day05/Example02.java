package com.atguigu.day05;

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
 * @Data 2022/6/289:08
 */
public class Example02 {
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


    public static class WindowResult extends ProcessWindowFunction<Event, userViewCountPerWindow,String, TimeWindow> {


        @Override
        public void process(String key, ProcessWindowFunction<Event, userViewCountPerWindow, String, TimeWindow>.Context ctx, Iterable<Event> elements, Collector<userViewCountPerWindow> out) throws Exception {
            //Iterable<Event> elements包含了属于窗口的所有元素
            //elements.spliterator().getExactSizeIfKnown()返回迭代器中的元素数量
            out.collect(new userViewCountPerWindow(
                    key,
                    elements.spliterator().getExactSizeIfKnown(),
                    ctx.window().getStart(),
                    ctx.window().getEnd()
            ));
        }
    }
}