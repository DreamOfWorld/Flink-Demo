package com.atguigu.day07;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/520:32
 */
public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> clickSource = env.addSource(new ClickSource());
        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999);
        clickSource
                .keyBy(r -> r.key)
                //数据流在广播之前，并行度一定要设置成1
                //因为数据需要按照顺序广播出去
                //为了使下游的并行子任务看到的都是相同的广播数据
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new CoFlatMapFunction<Event, String, Event>() {
                    //用来保存socket输入的查询字符串。初始值为空
                    private  String queryString = "";
                    @Override
                    public void flatMap1(Event event, Collector<Event> out) throws Exception {
                        if (event.key.equals(queryString)){
                            out.collect(event);
                        }
                    }

                    @Override
                    public void flatMap2(String s, Collector<Event> out) throws Exception {
                        //socket的数据进入flatMap1算子触发flatMap2的调用
                        //将socket输入的字符串保存在queryString中
                        queryString = s;
                    }
                })
                .print()
        ;
        env.execute();
    }
}
