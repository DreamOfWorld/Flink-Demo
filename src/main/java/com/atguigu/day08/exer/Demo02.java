package com.atguigu.day08.exer;

import com.atguigu.utils.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/623:04
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       env.enableCheckpointing(10 * 1000L);
       env.setStateBackend(new FsStateBackend("file:///D:\\project\\flink\\src\\main\\resources\\ckpts"));

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
