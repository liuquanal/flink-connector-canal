package com.aquanz.flink.connector.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

/**
 * 为flink提供数据的canal消费者
 *
 * @author a.q.z 2019/10/18 下午10:57
 */
public class FlinkCanalConsumer extends RichSourceFunction<FlatMessage> {
    private final Logger logger = LogManager.getLogger(FlinkCanalConsumer.class);

    private String destination;
    private Properties properties;
    private CanalConnector connector;
    private int batchSize;

    private boolean isCancel = false;

    public FlinkCanalConsumer(String destination, Properties properties) {
        this.destination = destination;
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String host = properties.getProperty("host", "127.0.0.1");
        String port = properties.getProperty("port", "11111");
        String user = properties.getProperty("user", "");
        String pwd = properties.getProperty("pwd", "");
        String timeout = properties.getProperty("timeout", "3600000");

        this.batchSize = Integer.parseInt(properties.getProperty("batchSize", "1024"));

        connector = new SimpleCanalConnector(
                new InetSocketAddress(host, Integer.parseInt(port)),
                destination, user, pwd, Integer.parseInt(timeout));
    }

    @Override
    public void run(SourceContext<FlatMessage> sourceContext) throws Exception {
        while (!isCancel) {
            try {
                connector.connect();
                connector.subscribe();
                while (!isCancel) {
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    boolean rollback = false;
                    if (batchId == -1 || size == 0) {
                        sleep(1000L);
                    } else {
                        rollback = process(sourceContext, message);
                    }

                    if (batchId != -1) {
                        if (rollback) {
                            logger.error("process error and rollback");
                            connector.rollback(batchId);
                            sleep(1000L);
                        } else {
                            connector.ack(batchId);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("connect canal error: {1}", e);
                sleep(5000L);
            } finally {
                closeCanal();
            }
        }
    }

    @Override
    public void cancel() {
        closeCanal();
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeCanal();
    }

    private void closeCanal() {
        try {
            isCancel = true;
            connector.disconnect();
            logger.info("canal closed");
        } catch (Exception e) {
            // ignore
        }
    }

    private boolean process(SourceContext<FlatMessage> sourceContext, Message message) {
        try {
            List<FlatMessage> flatMessageList = MQMessageUtils.messageConverter(message);
            flatMessageList.forEach(sourceContext::collect);
        } catch (Exception e) {
            logger.error("process error: {1}", e);
        }
        return false;
    }

    private void sleep(long t) {
        try {
            Thread.sleep(t);
        } catch (Exception e) {
            // ignore
        }
    }
}
