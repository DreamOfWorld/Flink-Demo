package com.atguigu.day05.Test;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/49:18
 */
public class demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                // a 1
                //a 2
                .readTextFile("D:\\project\\flink\\src\\main\\resources\\word.txt")
                //"a 1" ->("a, 1000L)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] array = s.split(" ");
                        return Tuple2.of(array[0],Long.parseLong(array[1])*1000L);
                    }
                })
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
                    @Override
                    public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long l) {
                                return element.f1;
                            }
                        };
                    }

                    @Override
                    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple2<String, Long>>() {
                            private long delay = 5000L;
                            private long maxTs = -Long.MIN_VALUE + delay + 1L;
                            @Override
                            public void onEvent(Tuple2<String, Long> element, long l, WatermarkOutput watermarkOutput) {
                               maxTs = Math.max(element.f1,maxTs);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                watermarkOutput.emitWatermark(new Watermark(maxTs - delay - 1L));
                            }
                        };
                    }
                })
                //在map算子输出的数据流中插入水位线m
                //默认每隔200ms插入一次水位线事件
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context ctx, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
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
