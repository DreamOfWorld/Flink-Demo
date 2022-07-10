package com.atguigu.day05;

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
 * @Data 2022/6/289:08
 */
public class Example01 {
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

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String,Event,userViewCountPerWindow> {
        private Long windowsSize;

        public MyTumblingProcessingTimeWindow(Long windowsSize) {
            this.windowsSize = windowsSize;
        }
        //key:Tuple2<窗口开始时间,窗口结束时间>
        //value:窗口所有元素组成的列表
        private MapState<Tuple2<Long,Long>, List<Event>> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>, List<Event>>(
                    "windowsinfo-elements", Types.TUPLE(Types.LONG,Types.LONG),Types.LIST(Types.POJO(Event.class))));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, userViewCountPerWindow>.Context context, Collector<userViewCountPerWindow> collector) throws Exception {
            //根据时间戳计算数据所属的窗口的开始时间
            long currTS = context.timerService().currentProcessingTime();
            long windowsStartTime = currTS - currTS % windowsSize;
            long windowsEndTime = windowsStartTime +  windowsSize;

            //窗口信息
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowsStartTime, windowsEndTime);
            //判断mapState是否含有windowInfo这个key
            // 也就是mapState中是否存在windowInfo这个窗口
            //如果不存在这个窗口，说明输入数据in是属于这个窗口的第一个元素
            if(!mapState.contains(windowInfo)){
                //新建列表
                ArrayList<Event> elements = new ArrayList<>();
                //将输入数据添加到列表中
                elements.add(event);
                //创建一个新窗口，窗口中只有一个元素
                mapState.put(windowInfo,elements);
            }
            //如果windowInfo已经存在，也就是窗口已经存在
            else {
                //直接将输入数据in添加到windowInfo对应的列表中
                mapState.get(windowInfo).add(event);
            }
            //注册（窗口结束时间-1毫秒）的定时器
            context.timerService().registerProcessingTimeTimer(windowsEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, userViewCountPerWindow>.OnTimerContext ctx, Collector<userViewCountPerWindow> out) throws Exception {
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime -windowsSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            String key = ctx.getCurrentKey();
            long count = mapState.get(windowInfo).size();
            out.collect(new userViewCountPerWindow(
                    key,count,windowStartTime,windowEndTime
            ));
            //销毁窗口
            mapState.remove(windowInfo);
        }
    }

}