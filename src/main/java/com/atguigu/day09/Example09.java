package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment
                .create(
                        env,
                        EnvironmentSettings.newInstance().inStreamingMode().build()
                );

        // 定义输入表
        // 将csv文件转换成动态表
        streamTableEnvironment
                .executeSql(
                        "create table clicks (`user` STRING, `url` STRING) " +
                                "WITH (" +
                                "'connector' = 'filesystem'," +
                                "'path' = '/home/zuoyuan/flinktutorial0106/src/main/resources/file.csv'," +
                                "'format' = 'csv'" +
                                ")"
                );

        // 定义输出表
        streamTableEnvironment
                .executeSql(
                        "create table ResultTable (`user` STRING, `cnt` BIGINT) " +
                                "WITH (" +
                                "'connector' = 'print'" +
                                ")"
                );

        // 在输入表里查询并将查询结果写入输出表
        streamTableEnvironment
                .executeSql(
                        "insert into ResultTable" +
                                " select user, count(user) from clicks group by user"
                );
    }
}
