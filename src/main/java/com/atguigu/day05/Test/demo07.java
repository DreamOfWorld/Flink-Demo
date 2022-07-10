package com.atguigu.day05.Test;

import com.atguigu.utils.ProductViewCountPerWindow;
import com.atguigu.utils.userBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

// 每件商品在每个窗口的浏览次数
// 每个窗口中浏览次数最多的商品
public class demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .readTextFile("D:\\project\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, userBehavior>() {
                    @Override
                    public userBehavior map(String s) throws Exception {
                        String[] elements = s.split(",");
                        return new userBehavior(
                                elements[0],
                                elements[1],
                                elements[2],
                                elements[3],
                                Long.parseLong(elements[4])  * 1000L
                        );
                    }
                })
                .filter(r -> r.type.contains("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<userBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<userBehavior>() {
                            @Override
                            public long extractTimestamp(userBehavior userBehavior, long l) {
                                return userBehavior.ts;
                            }
                        }))
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new countAgg(),new resultWindow())
                .keyBy(r -> r.windowEndTime)
                .process(new KeyedProcessFunction<Long, ProductViewCountPerWindow, String>() {
                    private ListState<ProductViewCountPerWindow> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                       listState = getRuntimeContext().getListState(new ListStateDescriptor<ProductViewCountPerWindow>(
                               "list-state",
                               Types.POJO(ProductViewCountPerWindow.class)
                       ));
                    }

                    @Override
                    public void processElement(ProductViewCountPerWindow in, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {
                        listState.add(in);
                        ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
                        for (ProductViewCountPerWindow productViewCountPerWindow : listState.get()) {
                            arrayList.add(productViewCountPerWindow);
                        }
                        arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                            @Override
                            public int compare(ProductViewCountPerWindow o1, ProductViewCountPerWindow o2) {
                                return (int) (o2.count - o1.count);
                            }
                        });
                        StringBuilder result = new StringBuilder();
                        result.append("========================\n");
                        ProductViewCountPerWindow topProduct = arrayList.get(0);
                        result.append("窗口：" +  new Timestamp(topProduct.windowStartTime) + "~" + new Timestamp(topProduct.windowEndTime) + "\n");
                        result.append("第一名：：" + topProduct + "\n");
                        result.append("========================\n");
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }

    public static class countAgg implements AggregateFunction<userBehavior,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(userBehavior userBehavior, Long aLong) {
            return aLong + 1L;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }
    public static class resultWindow extends ProcessWindowFunction<Long,ProductViewCountPerWindow,String,TimeWindow>{

        @Override
        public void process(String key, ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>.Context ctx, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(new ProductViewCountPerWindow(
                    key,
                    elements.iterator().next(),
                    ctx.window().getStart(),
                    ctx.window().getEnd()
            ));
        }
    }
}
