package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment
                .create(
                        env,
                        EnvironmentSettings.newInstance().inStreamingMode().build()
                );

        streamTableEnvironment
                .executeSql(
                        "create table clicks (" +
                                "`user` STRING, " +
                                "`url` STRING, " +
                                "ts BIGINT, " +
                                "time_ltz AS TO_TIMESTAMP_LTZ(ts, 3), " +
                                "WATERMARK FOR time_ltz AS time_ltz - INTERVAL '3' SECONDS) " +
                                "WITH (" +
                                "'connector' = 'filesystem'," +
                                "'path' = '/home/zuoyuan/flinktutorial0106/src/main/resources/file1.csv'," +
                                "'format' = 'csv')"
                );

        streamTableEnvironment
                .executeSql(
                        "create table ResultTable (" +
                                "`user` STRING, " +
                                "windowEndTime TIMESTAMP(3), " +
                                "cnt BIGINT)" +
                                " WITH (" +
                                "'connector' = 'print')"
                );

        streamTableEnvironment
                .executeSql(
                        "insert into ResultTable " +
                                "select user, " +
                                "TUMBLE_END(time_ltz, INTERVAL '1' HOURS) as windowEndTime, " +
                                "count(user) as cnt " +
                                "from clicks group by user, TUMBLE(time_ltz, INTERVAL '1' HOURS)"
                );
    }
}
