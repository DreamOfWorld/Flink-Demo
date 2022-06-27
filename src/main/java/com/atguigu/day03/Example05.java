package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                               @Override
                               public void run(SourceContext<Integer> sourceContext) throws Exception {
                                   for (int i = 1; i < 9; i++) {
                                       int idx = getRuntimeContext().getIndexOfThisSubtask();
                                       if (i % 2 == idx){
                                           sourceContext.collect(i);
                                       }
                                   }
                               }

                               @Override
                               public void cancel() {

                               }
                           }
                )
                .setParallelism(2)
                .rebalance()
                .print()
                .setParallelism(4);

        env.execute();


    }
}
