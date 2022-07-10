package com.atguigu.day06;


import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @Description
 * @Author mei
 * @Data 2022/6/289:08
 */
public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.emitWatermark(new Watermark(999L));
                        ctx.collectWithTimestamp("a", 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp("a", 1500L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        if (context.timestamp() > context.timerService().currentWatermark()) {
                            collector.collect("数据" + s + "的事件时间是：" + context.timestamp() + ", " + "当前水位线是:" + context.timerService().currentWatermark());
                        } else {
                            context.output(new OutputTag<String>("late-event") {
                            }, "数据" + s + "的事件时间是：" + context.timestamp()
                                    + ", 当前水位线是：" + context.timerService().currentWatermark());
                        }
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<String>("late-event"){}).print("测输出流");

        env.execute();
    }


}