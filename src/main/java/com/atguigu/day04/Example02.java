package com.atguigu.day04;

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
 * @Data 2022/6/289:08
 */
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
                    //如果flag为空，说明此时不存在定时器
                    //如果flag不为空，说明此时存在定时器
                    private ValueState<Integer> flag;
                    //定时器用来向下游发送统计结果
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Integer, Statistic>.OnTimerContext ctx, Collector<Statistic> out) throws Exception {
                        out.collect(accumulator.value());
                        //发送完统计结果之后，将flag置为空
                        //这样就能继续住注册定时器了
                        flag.clear();
                    }

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
                                    (oldAcc.sum + in)/oldAcc.count + 1
                                    ));
                        }
                        //只有在不存在定时器的情况下才注册定时器
                        if (flag.value() == null){
                            context.timerService().registerEventTimeTimer(
                                    context.timerService().currentProcessingTime()
                            );
                            flag.update(1);
                        }
                    }
                })

                .print()
        ;
        env.execute();
    }
    public static class Statistic {
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
