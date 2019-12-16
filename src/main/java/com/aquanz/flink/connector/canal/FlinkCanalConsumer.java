package com.aquanz.flink.connector.canal;

import java.util.List;
import java.util.Properties;

import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 为flink提供数据的canal消费者
 *
 * @author a.q.z 2019/10/18 下午10:57
 */
public class FlinkCanalConsumer extends RichSourceFunction<FlatMessage> {
    /**
     *
     */
    private static final long serialVersionUID = -4067643991085518633L;

    private final Logger logger = LogManager.getLogger(FlinkCanalConsumer.class);

    private CanalServerWithEmbedded server;
    private ClientIdentity clientIdentity = new ClientIdentity(InstanceGen.DESTINATION, (short) 1);

    private boolean isCancel = false;

    public FlinkCanalConsumer(Properties properties) {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        server = CanalServerWithEmbedded.instance();
        server.setCanalInstanceGenerator(destination -> {
            Canal canal = InstanceGen.generate();
            return new CanalInstanceWithManager(canal, InstanceGen.FILTER);
        });
        server.setMetricsPort(1112);
        server.start();
        server.start(InstanceGen.DESTINATION);
        server.subscribe(clientIdentity);
    }

    @Override
    public void run(SourceContext<FlatMessage> sourceContext) throws Exception {
        while (!isCancel) {
            Message message = server.getWithoutAck(clientIdentity, 11);
            // List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(message);
            // System.out.println(JSONObject.toJSONString(message));
            // System.out.println("收到消息：\n" + JSONObject.toJSONString(flatMessages));
            long msgId = message.getId();
            if (msgId != -1) {
                try {
                    if (CollectionUtils.isNotEmpty(message.getRawEntries())) {
                        process(sourceContext, message);
                        server.ack(clientIdentity, message.getId());
                        sleep(100L);
                        continue;
                    }
                } catch (Exception e) {
                    try {
                        server.rollback(clientIdentity);
                    } catch (Exception e1) {
                        // ignore
                    }
                }
            }
            sleep(3000L);
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
        isCancel = true;
        if (server != null) {
            server.unsubscribe(clientIdentity);
            server.stop();
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
