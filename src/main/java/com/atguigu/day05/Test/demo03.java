package com.atguigu.day05.Test;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.userViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/321:20
 */
public class demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg(),new windowResult())
                .print();
        env.execute();
    }
    public static class CountAgg implements AggregateFunction<Event,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }
    public static class windowResult extends ProcessWindowFunction<Long,userViewCountPerWindow,String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Long, userViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<userViewCountPerWindow> collector) throws Exception {
            collector.collect(new userViewCountPerWindow(
                    s,iterable.iterator().next(), context.window().getStart(),context.window().getEnd()
            ));
        }
    }
}
