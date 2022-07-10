package com.atguigu.Exercise;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2815:06
 */
public class demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private MapState<String,Integer> keyCount;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                       keyCount = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("key-Count", Types.STRING,Types.INT));
                    }

                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        if (!keyCount.contains(event.value)){
                            keyCount.put(event.value,1);
                        }else {
                            Integer oldCount = keyCount.get(event.value);
                            keyCount.put(event.value,oldCount + 1);
                        }

                        StringBuilder result = new StringBuilder();
                        result.append(context.getCurrentKey() + "{\n");
                        for (String key : keyCount.keys()) {
                            result.append(" ").append("\"" + key + "\" ->").append(keyCount.get(key) + ",\n");
                        }
                        result.append("}\n");
                        collector.collect(result.toString());
                    }
                })
                .print();
        env.execute();
    }
}
