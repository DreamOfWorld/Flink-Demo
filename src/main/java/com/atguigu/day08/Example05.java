package com.atguigu.day08;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        env
                .addSource(new CounterSource())
                .addSink(new TransactionalFileSink());

        env.execute();
    }

    public static class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
        private boolean running = true;

        private Long offset = 0L;

        // 声明一个算子状态变量
        // 列表状态变量中，只保存一个元素，保存刚消费完的偏移量
        // 算子状态不能使用ValueState
        private ListState<Long> state;
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();

            while (running) {
                // 加锁
                synchronized (lock) {
                    ctx.collect(offset);
                    offset += 1L;
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        // 程序启动时触发调用（不管是第一次启动还是故障恢复）
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            state = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>(
                            "state",
                            Types.LONG
                    )
            );

            // 如果state中不为空，说明程序是故障恢复之后的启动，所以里面的元素就是刚消费完的偏移量
            // 如果程序是第一次启动，循环不执行
            // 如果是故障恢复启动，那么循环执行一次
            for (Long l : state.get()) {
                offset = l;
            }
        }

        // 当检查点分界线进入source算子时触发调用
        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // 保存刚消费完的偏移量
            state.clear();
            state.add(offset);
        }
    }

    // 开启事务：创建临时文件
    // 预提交：将数据输出到临时文件
    // 正式提交：将临时文件名改成正式文件名
    // 回滚：删除临时文件
    public static class TransactionalFileSink extends TwoPhaseCommitSinkFunction<Long, String, Void> {
        // 初始化一个缓冲区
        private BufferedWriter transactionWriter;

        public TransactionalFileSink() {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        // 每来一条数据触发一次调用
        @Override
        protected void invoke(String filename, Long in, Context context) throws Exception {
            transactionWriter.write(in + "\n"); // 将输入数据写入缓冲区
        }

        // 每个检查点对应一个事务
        @Override
        protected String beginTransaction() throws Exception {
            long timeNow = System.currentTimeMillis();
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String filename = timeNow + "-" + taskIdx;
            // 创建临时文件
            Path tempFilePath = Paths.get("/home/zuoyuan/filetemp/" + filename);
            Files.createFile(tempFilePath);
            // 将缓冲区指定为临时文件的写入缓冲区
            this.transactionWriter = Files.newBufferedWriter(tempFilePath);
            return filename;
        }

        // 预提交的逻辑
        @Override
        protected void preCommit(String filename) throws Exception {
            transactionWriter.flush(); // 将数据刷入临时文件
            transactionWriter.close(); // 关闭缓冲区
        }

        // 正式提交的逻辑
        @Override
        protected void commit(String filename) {
            Path tempFilePath = Paths.get("/home/zuoyuan/filetemp/" + filename);
            if (Files.exists(tempFilePath)) {
                try {
                    Path commitFilePath = Paths.get("/home/zuoyuan/filetarget/" + filename);
                    // 将临时文件名改成正式文件名
                    Files.move(tempFilePath, commitFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 回滚
        @Override
        protected void abort(String filename) {
            Path tempFilePath = Paths.get("/home/zuoyuan/filetemp/" + filename);
            if (Files.exists(tempFilePath)) {
                try {
                    // 删除临时文件
                    Files.delete(tempFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
