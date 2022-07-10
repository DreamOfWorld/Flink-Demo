package com.atguigu.day05.Test;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.userViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


/**
 * @Description
 * @Author mei
 * @Data 2022/6/2915:52
 */
public class demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .process(new MyTumblingProcessingTimeWindow(10*1000L))
                .print();
        env.execute();
    }

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, Event, userViewCountPerWindow>{
        private Long windowSize;

        public MyTumblingProcessingTimeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }
        private MapState<Tuple2<Long,Long>, List<Event>> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
           mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>, List<Event>>(
                   "windowInfo-elements",
                   Types.TUPLE(Types.LONG,Types.LONG),
                   Types.LIST(Types.POJO(Event.class))
           ));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, userViewCountPerWindow>.Context context, Collector<userViewCountPerWindow> collector) throws Exception {
            long currTs = context.timerService().currentProcessingTime();
            long startTime = currTs - currTs % windowSize;
            long endTime = startTime + windowSize;

            Tuple2<Long, Long> windowInfo = Tuple2.of(startTime, endTime);
            if (!mapState.contains(windowInfo)){
                ArrayList<Event> elements = new ArrayList<>();
                elements.add(event);
                mapState.put(windowInfo,elements);
            }else {
                mapState.get(windowInfo).add(event);
            }

            context.timerService().registerProcessingTimeTimer(endTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, userViewCountPerWindow>.OnTimerContext ctx, Collector<userViewCountPerWindow> out) throws Exception {
            long endTime = timestamp + 1L;
            long startTime = endTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(startTime, endTime);

            long count = mapState.get(windowInfo).size();
            String key = ctx.getCurrentKey();
            out.collect(new userViewCountPerWindow(key,count,startTime,endTime));

            mapState.remove(windowInfo);
        }
    }
}
