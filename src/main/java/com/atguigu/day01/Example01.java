package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2119:45
 */
public class Example01 {
    //不要忘记抛出异常
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                //数据源是socket
                //先启动‘nc -lk 9999'
                .socketTextStream("localhost",9999)
                //数据源的并行子任务的数量是1
                .setParallelism(1)
                //map阶段
                //"hello world" =>("hello",1),("world",1)
                //由于这里一个字符串可能会被转换成多个元组输出
                .flatMap(new Tokenizer())
                //flatMap的并行子任务的数量是1
                .setParallelism(1)
                //shuffle阶段
                // 将数据使用key进行分组，key是单词
                //f0是元组的第0个元组，相当于scala中’_1'
                .keyBy(r -> r.f0)
                //reduce阶段
                //将key相同的元组的第一个字段('f1')进行聚合
                .reduce(new Sum())
                //freduce的并行子任务的数量是1
                .setParallelism(1)
                //输出聚合
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
    //Reduce只有一个泛型
    //因为reduce的输入、累加器和输出类型都是一样的
    public static class Sum implements ReduceFunction<Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in, Tuple2<String, Integer> accumulator) throws Exception {
            //定义输入数据和累加器的聚合规则
            //返回值是新的累加器，会覆盖旧的累加器
            //reduce算子的输出时累加器的值
            return  Tuple2.of(in.f0, in.f1 + accumulator.f1);
        }
    }
}
