package com.atguigu.day05.Test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/49:18
 */
public class demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        ctx.collectWithTimestamp(1,1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> "number")
                .process(new KeyedProcessFunction<String, Integer, String>() {
                    @Override
                    public void processElement(Integer in, KeyedProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                        out.collect("数据： " + in + " 到达KeyedProcessFunction的并行子任务的水位线是： " + "" + ctx.timerService().currentWatermark());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Integer, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("时间戳： " + timestamp + " 到达KeyedProcessFunction的并行子任务的水位线是： " + "" + ctx.timerService().currentWatermark());
                    }
                })
                .print();
        env.execute();
    }
}
