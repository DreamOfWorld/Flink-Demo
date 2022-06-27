package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2119:45
 */
public class Example02 {
    //不要忘记抛出异常
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .readTextFile("D:\\project\\flink\\src\\main\\resources\\word.txt")
                .flatMap(new Tokenizer())
                .keyBy(r -> r.f0)
                .sum("f1")
                .print();
        //提交并执行环境
        env.execute();
    }
    //FlatMapFunction<IN,OUT>
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        //out是集合，用来收集向下游发送的数据
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            //使用
            String[] words = in.split(" ");
            //将单词转换为元组，并收集到集合中
            //flink会自动将集合中的数据向下发送
            for (String word : words) {
                out.collect(Tuple2.of(word,1));
            }

        }
    }
}
