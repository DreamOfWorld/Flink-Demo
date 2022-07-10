package com.atguigu.day09.exer;

import com.atguigu.utils.userBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

//连续登陆三次失败的检测
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<userBehavior> stream = env
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
                        }));
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());
        Table table = tableEnvironment
                .fromDataStream(
                        stream,
                        $("userId"),
                        $("productId"),
                        $("categoryId"),
                        $("type"),
                        $("ts").rowtime()
                );
        tableEnvironment.createTemporaryView("userBehavior",table);
        Table result = tableEnvironment
                .sqlQuery(
                        " SELECT productId, COUNT(productId) as cnt, " +
                                " HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowStartTime, " +
                                " HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowEndTime " +
                                " FROM userBehavior GROUP BY " +
                                " productId, HOP(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS)"
                );
        DataStream<Row> stream1 = tableEnvironment.toChangelogStream(result);
        stream1.print();
        env.execute();
    }
}