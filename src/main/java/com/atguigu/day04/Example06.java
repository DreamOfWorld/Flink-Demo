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
public class Example06 {
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
    //思路：
    //温度出现上升，注册1秒钟之后的报警定时器
    //温度出现下降,如果报警定时器还存在，那么说明是1秒钟之内出现了温度下降。删除报警定时器
    public static class TempAlter extends KeyedProcessFunction<String,SensorReading,String>{
        //保存上一次的温度
        private ValueState<Double> lastTemp;
        //保存报警定时器的时间戳
        //如果不为空，表示当前存在报警定时器
        private ValueState<Long> timerTs;
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE)
            );
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-Ts",Types.LONG));
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context context, Collector<String> collector) throws Exception {
            //首先取出上一次的温度
            //如果到达的数据是第一条温度，那么preTemp是null
            Double prevTemp = lastTemp.value();
            //将当前温度值保存到lastTemp
            lastTemp.update(sensorReading.temperature);
            //取出报警定时器的时间戳
            //如果报警定时器不存在,那么ts为null
            Long ts = timerTs.value();
            //需要保证上一次温度是存在的
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

        //报警定时器
        //由于触发onTimer执行时，flink会自动将定时器队列中删除
        //所以这里需要将保存定时器时间戳的timerTs删除
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器"+ctx.getCurrentKey()+"连续1s温度上升了！");
            timerTs.clear();
        }
    }
    public static class SensorSource implements SourceFunction<SensorReading>{
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
            running =false;
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
