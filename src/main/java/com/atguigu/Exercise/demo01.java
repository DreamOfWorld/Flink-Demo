package com.atguigu.Exercise;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2517:19
 */
public class demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .readTextFile("src/main/resources/word.txt")
                .flatMap(new Tokenizer())
                .keyBy(r -> r.f0)
                .sum(1)
                .print();

        env.execute();
    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word,1));
            }

        }
    }
}
