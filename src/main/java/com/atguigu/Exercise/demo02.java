package com.atguigu.Exercise;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Description
 * @Author mei
 * @Data 2022/6/2721:13
 */
public class demo02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new ClickSource())
                .addSink(new MyJDBC())
        ;

    }
    public static class MyJDBC extends RichSinkFunction<Event> {
        private Connection connection;
        private PreparedStatement insertStatement;
        private PreparedStatement updateStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/userbehavior?userSSL=false&&serverTimezone=GMT","root","my19970929");
            insertStatement = connection.prepareStatement("insert into cliks (username,url)values (?,?)");
            updateStatement = connection.prepareStatement("update clicks set url = ? where username = ?");

        }

        @Override
        public void close() throws Exception {
            updateStatement.close();
            insertStatement.close();
            connection.close();
        }

        @Override
        public void invoke(Event in, Context context) throws Exception {
            updateStatement.setString(1,in.value);
            updateStatement.setString(2,in.key);
            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0){
                insertStatement.setString(1,in.key);
                insertStatement.setString(2,in.value);
                insertStatement.execute();
            }
        }
    }
}
