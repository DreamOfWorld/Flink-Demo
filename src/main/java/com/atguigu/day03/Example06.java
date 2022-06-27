package com.atguigu.day03;

import com.atguigu.day02.ClickSource;
import com.atguigu.day02.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2718:32
 */
public class Example06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .addSource(new ClickSource())
                .addSink(new MyJDBC())

        ;


        env.execute();


    }
    public static class MyJDBC extends RichSinkFunction<Event> {
        private  Connection connection;
        private  PreparedStatement insertStatement;//插入语句
        private  PreparedStatement updateStatement;//更新语句

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/userbehavior?userSSL=false&&serverTimezone=GMT","root","my19970929");
            insertStatement = connection.prepareStatement("insert into clicks (username,url) values (?,?)");
            updateStatement = connection.prepareStatement("update clicks set url = ? where username = ?");
        }
        //每来一条数据，触发一次调用
        //幂等性地写入MySQL
        //如果某个username的数据已经存在，则覆盖
        //如果username不存在，则插入数据
        @Override
        public void invoke(Event in, Context context) throws Exception {
            //执行更新语句
            updateStatement.setString(1,in.value);
            updateStatement.setString(2,in.key);
            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0){
                insertStatement.setString(1,in.key);
                insertStatement.setString(2,in.value);
                insertStatement.execute();
            }
        }

        //
        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
