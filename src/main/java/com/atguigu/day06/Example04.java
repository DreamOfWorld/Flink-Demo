package com.atguigu.day06;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * @Description
 * @Author mei
 * @Data 2022/6/289:08
 */
public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop012:9092");
        properties.put("group.id","consumer-group");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.stringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.stringDeserializer");
        properties.put("auto.offset.reset","lastest");
        env
                .addSource(new FlinkKafkaConsumer<String>("userbehavior-0106",new SimpleStringSchema(),properties))
                .print()
        ;

        env.execute();
    }


}