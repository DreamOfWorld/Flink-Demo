package com.atguigu.day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/49:18
 */
public class Example07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        //flink会在run方法执行之前发出一个-max的水位线
                        //flink会在run方法执行完毕之后发出一个max的水位线
                        ctx.emitWatermark(new Watermark(5000L));
                        ctx.collectWithTimestamp(1,6000L);
                        ctx.emitWatermark(new Watermark(10 * 1000L));
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> "number")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Integer, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Integer, String, String, TimeWindow>.Context ctx, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        out.collect("key: " + key + ",在窗口" +
                                "" + ctx.window().getStart() + "~" +
                                "" + ctx.window().getEnd() + "里面有 "+
                                "" + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                })
                .print();
        env.execute();
    }
}
