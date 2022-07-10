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
 * @Data 2022/6/2816:33
 */
public class demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TempAlter())
                .print();
        env.execute();
    }
    public static class TempAlter extends KeyedProcessFunction<String, SensorReading,String>{
        private ValueState<Double> lastTemp;
        private ValueState<Long> timerTs;
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Types.LONG));
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context context, Collector<String> collector) throws Exception {
            Double prevTemp = lastTemp.value();
            lastTemp.update(sensorReading.temperature);
            Long ts = timerTs.value();
            if (prevTemp != null){
                if (prevTemp < sensorReading.temperature && ts == null){
                    long time = context.timerService().currentProcessingTime() + 1000L;
                    context.timerService().registerProcessingTimeTimer(time);
                    timerTs.update(time);
                } else if (prevTemp > sensorReading.temperature && ts !=null) {
                    context.timerService().deleteEventTimeTimer(timerTs.value());
                    timerTs.clear();
                }
            }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey() +"连续1s温度上升了！");
            timerTs.clear();
        }
    }
    public static class SensorSource implements SourceFunction<SensorReading> {
        private Boolean running = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            while (running){
                for (int i = 1; i < 4; i++) {
                    sourceContext.collect(new SensorReading(
                            "sensor_" + i,
                            random.nextGaussian()
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
    public static class SensorReading{
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }
    }
}
