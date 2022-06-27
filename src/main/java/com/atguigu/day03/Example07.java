package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
public class Example07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .fromElements("white","Black","red")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        if (s.equals("white")){
                            collector.collect(s);
                        } else if (s.equals("Black")) {
                            collector.collect(s);
                            collector.collect(s);
                        }
                    }
                })
                .print()
        ;

        env.execute();


    }
}
