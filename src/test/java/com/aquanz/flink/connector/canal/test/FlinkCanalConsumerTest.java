package com.aquanz.flink.connector.canal.test;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.aquanz.flink.connector.canal.FlinkCanalConsumer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * flink的canal消费者测试
 *
 * @author a.q.z 2019/10/19 上午12:02
 */
public class FlinkCanalConsumerTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        Properties properties = new Properties();
        FlinkCanalConsumer myConsumer = new FlinkCanalConsumer("example", properties);
        DataStreamSource<FlatMessage> stream = env.addSource(myConsumer);

        stream.print();

        env.execute("FlinkCanalConsumer Test");
    }

}
