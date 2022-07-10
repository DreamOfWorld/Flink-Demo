package com.atguigu.day08;

import com.atguigu.utils.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每隔10s保存一次检查点
        env.enableCheckpointing(10 * 1000L);
        // 设置检查点文件夹的绝对路径
        // file:// + 文件夹的绝对路径
        env.setStateBackend(new FsStateBackend("file:///D:\\project\\flink\\src\\main\\resources\\ckpts"));

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
