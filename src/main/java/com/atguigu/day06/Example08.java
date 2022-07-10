package com.atguigu.day06;


import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @Description
 * @Author mei
 * @Data 2022/6/289:08
 */
public class Example08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collect("a");
                        Thread.sleep(1000L);
                        ctx.collect("a");
                        Thread.sleep(10 * 1000L);
                        ctx.collect("a");
                        Thread.sleep(10 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> true)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean key, ProcessWindowFunction<String, String, Boolean, TimeWindow>.Context ctx, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("key: " + key + ",在窗口" +
                                "" + ctx.window().getStart() + "~" +
                                "" + ctx.window().getEnd() + "里面有 " +
                                "" + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                })
                ;
        result.print();
        result.getSideOutput(new OutputTag<String>("late-event"){}).print("测输出流");

        env.execute();
    }

}