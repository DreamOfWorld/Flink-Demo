package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
//使用keyedProcessFunction维护的内部状态是：定时器
public class Example09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .socketTextStream("localhost",9999)
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {


                    @Override
                    public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                        //获取processElement方法调用的机器时间
                        long currentProcessingTime = context.timerService().currentProcessingTime();
                        //获取10s之后的时间戳
                        long tenSecondLater = currentProcessingTime + 10 * 1000L;
                        //注册10s后的定时器
                        context.timerService().registerEventTimeTimer(tenSecondLater);

                        //获取60s之后的时间戳
                        long sixtySecondLater = currentProcessingTime + 10 * 1000L;
                        //注册60s后的定时器
                        context.timerService().registerEventTimeTimer(sixtySecondLater);

                        collector.collect("数据" + s + "到达，到达的机器时间是：" + new Timestamp(currentProcessingTime) +" ,注册了第一个定时器，时间戳是： " + new Timestamp(tenSecondLater) +" ,注册了第二个定时器，时间戳是： " + new Timestamp(sixtySecondLater));
                    }
                    //`timestamp`参数是上面注册的定时器的时间戳tenSecondLater
                    //当机器时间超过`timestamp`，则触发onTimer的执行
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("时间戳是： " + new Timestamp(timestamp) + "的定时器出发了！onTimer执行的机器时间是：" + new Timestamp(ctx.timerService().currentProcessingTime()));
                    }
                    }
                        )
                .print()
        ;

        env.execute();


    }
}
