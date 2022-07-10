package com.atguigu.day06;

import com.atguigu.day05.Example08;
import com.atguigu.utils.ProductViewCountPerWindow;
import com.atguigu.utils.userBehavior;
import org.apache.commons.lang3.text.StrBuilder;
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


/**
 * @Description
 * @Author mei
 * @Data 2022/6/289:08
 */
public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\project\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, userBehavior>() {
                    @Override
                    public userBehavior map(String s) throws Exception {
                        String[] array = s.split(",");
                        return new userBehavior(
                                array[0],
                                array[1],
                                array[2], array[3],
                                Long.parseLong(array[4]) * 1000L);
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<userBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<userBehavior>() {
                            @Override
                            public long extractTimestamp(userBehavior userBehavior, long l) {
                                return userBehavior.ts;
                            }
                        }))
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new Example08.CountAgg(), new Example08.WindowResult())
                .keyBy(r -> r.windowEndTime)
                //按照窗口信息对（每个商品在每个窗口的浏览器次数）这条流进行分组
                .process(new TopN(3))
                .print();
        env.execute();
    }

    public static class CountAgg implements AggregateFunction<userBehavior, Long, Long> {

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

    public static class WindowResult extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {


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

    public static class TopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {
        private int n;

        public TopN() {
        }

        public TopN(int n) {
            this.n = n;
        }
        private ListState<ProductViewCountPerWindow> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ProductViewCountPerWindow>(
                    "list-state",
                    Types.POJO(ProductViewCountPerWindow.class)
            ));
        }

        @Override
        public void processElement(ProductViewCountPerWindow productViewCountPerWindow, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.Context context, Collector<String> collector) throws Exception {
            listState.add(productViewCountPerWindow);
            //加1000ms是为了保证所有的ProductViewCountPerWindow全部到达
            context.timerService().registerEventTimeTimer(productViewCountPerWindow.windowEndTime + 1000L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //将ListState中的数据取出并放入ArrayList
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow count : listState.get()) {
                arrayList.add(count);
            }
            //由于listState中的数据已经没用了，所以清空
            listState.clear();
            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow o1, ProductViewCountPerWindow o2) {
                    return (int)(o2.count - o1.count);
                }
            });
            StrBuilder result = new StrBuilder();
            result.append("===========================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1000L) + "\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow tmp = arrayList.get(i);
                result.append("第" + (i + 1) + "名的商品ID是：" + tmp.productId + ",浏览次数是：" + tmp.count + "\n");
            }
            result.append("===========================\n");
            out.collect(result.toString());
        }
    }
}