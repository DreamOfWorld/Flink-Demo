package com.atguigu.day06;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;


/**
 * @Description
 * @Author mei
 * @Data 2022/6/289:08
 */
public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop012:9092");
        env
                .readTextFile("D:\\project\\flink\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>("userbehavior0106", new SimpleStringSchema(),properties))
        ;

        env.execute();
    }


}