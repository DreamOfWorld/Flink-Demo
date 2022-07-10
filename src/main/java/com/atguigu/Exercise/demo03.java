package com.atguigu.Exercise;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2810:13
 */
public class demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new SourceFunction<Integer>() {
                    private Boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running){
                            sourceContext.collect(random.nextInt(1000));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, Integer, Statistic>() {
                    private ValueState<Statistic> accumulator;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        accumulator = getRuntimeContext().getState(new ValueStateDescriptor<Statistic>("acc", Types.POJO(Statistic.class)));
                    }

                    @Override
                    public void processElement(Integer in, KeyedProcessFunction<Boolean, Integer, Statistic>.Context context, Collector<Statistic> collector) throws Exception {
                        if (accumulator.value() == null){
                            accumulator.update(new Statistic(in,in,in,1,in));
                        }else {
                            Statistic oldAcc = accumulator.value();
                            accumulator.update(new Statistic(
                                    Math.min(oldAcc.min,in),
                                    Math.max(oldAcc.max,in),
                                    oldAcc.sum + in,
                                    oldAcc.count + 1,
                                    (oldAcc.sum + in)/(oldAcc.count + 1))
                            );

                        }
                        collector.collect(accumulator.value());
                    }
                })
                .print()
        ;
        env.execute();
    }

    public static class Statistic{
        public Integer min;
        public Integer max;
        public Integer sum;
        public Integer count;
        public Integer avg;

        public Statistic() {
        }

        public Statistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "Statistic{" +
                    "min=" + min +
                    ", max=" + max +
                    ", sum=" + sum +
                    ", count=" + count +
                    ", avg=" + avg +
                    '}';
        }
    }

}
