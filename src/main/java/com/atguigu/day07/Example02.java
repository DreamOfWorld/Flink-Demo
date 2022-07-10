package com.atguigu.day07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:32
 */
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, String>> stream1 = env.fromElements(
                Tuple2.of("a", "left-1"),
                Tuple2.of("a", "left-2"),
                Tuple2.of("b", "left-1")
                );
        DataStreamSource<Tuple2<String, String>> stream2 = env.fromElements(
                Tuple2.of("a", "right-1"),
                Tuple2.of("b", "right-1"),
                Tuple2.of("b", "right-2")
        );
        stream1
                .keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    //history1用来保存来自第一条流的数据
                    private ListState<Tuple2<String,String>> history1;
                    //history2用来保存来自第一条流的数据
                    private ListState<Tuple2<String,String>> history2;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        history1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String,String>>(
                                "history1",
                                Types.TUPLE(Types.STRING,Types.STRING)
                        ));
                        history2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String,String>>(
                                "history2",
                                Types.TUPLE(Types.STRING,Types.STRING)
                        ));
                    }

                    @Override
                    public void processElement1(Tuple2<String, String> in1, CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                       //将in1添加到in1的key所对应的列表状态变量中
                        history1.add(in1);
                        //in1和in1的key所对应的history2列表状态变量中的所有的所有元素做join
                        for (Tuple2<String, String> right : history2.get()) {
                            out.collect(in1 + " -> " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, String> in2, CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                        //将in2添加到in2的key所对应的列表状态变量中
                        history2.add(in2);
                        //in2和in2的key所对应的history1列表状态变量中的所有的所有元素做join
                        for (Tuple2<String, String> left : history1.get()) {
                            out.collect(left + " -> " + in2);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
