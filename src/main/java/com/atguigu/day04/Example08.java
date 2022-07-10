package com.atguigu.day04;

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
 * @Data 2022/6/289:08
 */
public class Example08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //当AggregateFunction和ProcessWindowFunction结合使用时，需要调用aggregate方法
                .aggregate(
                        new CountAgg(),
                        new WindowResult()
                )
                .print();
        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Event,Long,Long> {

        //创建窗口时，初始化一个累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        //每来一条数据，累加器加1
        @Override
        public Long add(Event event, Long aLong) {
            return aLong + 1L;
        }
        //窗口闭合时，触发调用
        //将返回值发送出去
        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }
    //输入的泛型是Long,也就是AggregateFunction输出的泛型
    public static class WindowResult extends ProcessWindowFunction<Long,userViewCountPerWindow,String,TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Long, userViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<userViewCountPerWindow> collector) throws Exception {
            //Iterable<Long> iterable中只有一个元素，也就是getResult的返回值
            collector.collect(new userViewCountPerWindow(
                    s,
                    iterable.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

}