package com.atguigu.day09;

import com.atguigu.utils.userBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

//连续登陆三次失败的检测
public class Example08 {
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
        //获取表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());
        //将数据流转换成动态表
        Table table = streamTableEnvironment.fromDataStream(
                stream,
                $("userId"),
                $("productId"),
                $("categoryId"),
                $("type"),
                //rowtime()表示这一列是事件时间
                $("ts").rowtime()
        );
        //将动态表注册为一个临时视图
        streamTableEnvironment.createTemporaryView("userBehavior",table);
        //查询
        //HOP(时间戳的列名，滑动距离，窗口长度）
        //COUNT操作将窗口中的所有元素都收集起来然后数一遍
        //相当于只是用ProcessWindowFunction的情况
        //计算每个商品在每个窗口的浏览次数
        String innerSQL = " SELECT productId,COUNT(productId) as cnt, " +
                                " HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowStartTime, " +
                                " HOP_END(ts,INTERVAL '5' MINUTES ,INTERVAL '1' HOURS) as windowEndTime " +
                                " FROM userBehavior GROUP BY " +
                                " productId, " +
                                " HOP(ts,INTERVAL '5' MINUTES ,INTERVAL '1' HOURS) ";

        String midSQL = "SELECT *, ROW_NUMBER() OVER (PARTITION BY windowEndTime ORDER BY cnt DESC) as row_num " +
                "FROM (" + innerSQL + ")";

        String outerSQL = "SELECT * FROM (" + midSQL + ") WHERE row_num <= 3";
        Table result = streamTableEnvironment.sqlQuery(outerSQL);



        //+I表示INSERT
        streamTableEnvironment.toChangelogStream(result).print();
        env.execute();
    }
}