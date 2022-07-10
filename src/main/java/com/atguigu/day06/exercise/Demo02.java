package com.atguigu.day06.exercise;

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
 * @Data 2022/7/514:44
 */
public class Demo02 {
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
                    public void processElement(String in, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() > ctx.timerService().currentWatermark()) {
                            out.collect("数据" + in + "事件时间是：" + ctx.timestamp() + ", " + "当前水位线是:" + ctx.timerService().currentWatermark())
                            ;
                        } else {
                            ctx.output(new OutputTag<String>("late-event") {
                            }, "数据" + in + "的事件时间是：" + ctx.timestamp()
                                    + ", 当前水位线是：" + ctx.timerService().currentWatermark());
                        }
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<String>("late-event"){}).print("侧输出流");
        env.execute();
    }
}
