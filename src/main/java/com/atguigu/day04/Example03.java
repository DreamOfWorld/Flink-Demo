package com.atguigu.day04;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/289:08
 */
public class Example03 {
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
                .keyBy(r -> r%2)
                .process(new KeyedProcessFunction<Integer, Integer, String>() {
                    private ListState<Integer> history;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        history = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("history", Types.INT));
                    }

                    @Override
                    public void processElement(Integer in, KeyedProcessFunction<Integer, Integer, String>.Context context, Collector<String> collector) throws Exception {
                        //???????????????in?????????in???key????????????ListState???
                        history.add(in);
                        //????????????ListState?????????
                        //?????????ArrayList?????????
                        ArrayList<Integer> integers = new ArrayList<>();
                        //history.get()????????????in???key????????????ListState??????????????????
                        for (Integer i : history.get()) {
                            integers.add(i);
                        }
                        //??????
                        integers.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o1 - o2 ;
                            }
                        });

                        //???????????????
                        StringBuilder result = new StringBuilder();
                        if (context.getCurrentKey() == 0){result.append("????????????????????? ");
                        }else {
                            result.append("????????????????????? ");
                        }
                        for (Integer i : integers) {
                            result.append(i + " -> ");
                        }
                        collector.collect(result.toString());
                    }
                })

                .print()
        ;
        env.execute();
    }
}
