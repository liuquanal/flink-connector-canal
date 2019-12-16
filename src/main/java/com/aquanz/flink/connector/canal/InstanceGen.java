package com.aquanz.flink.connector.canal;

import java.net.InetSocketAddress;
import java.util.Arrays;

import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;

/**
 * Canal实例生成工具
 *
 * @author a.q.z 2019/12/15 上午11:33
 */
public class InstanceGen {

    public static final String DESTINATION = "example";
    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "127.0.0.1";
    private static final String USERNAME = "canal";
    private static final String PASSWORD = "canal";
    public static final String FILTER = ".*\\..*";

    public static Canal generate() {
        Canal canal = new Canal();
        canal.setId(1L);
        canal.setName(DESTINATION);
        canal.setDesc("mysql canal server");

        CanalParameter parameter = new CanalParameter();

        parameter.setMetaMode(CanalParameter.MetaMode.LOCAL_FILE);
        parameter.setDataDir("./conf");
        parameter.setMetaFileFlushPeriod(1000);
        parameter.setHaMode(CanalParameter.HAMode.HEARTBEAT);
        parameter.setIndexMode(CanalParameter.IndexMode.MEMORY_META_FAILBACK);

        parameter.setStorageMode(CanalParameter.StorageMode.MEMORY);
        parameter.setMemoryStorageBufferSize(32 * 1024);

        parameter.setSourcingType(CanalParameter.SourcingType.MYSQL);
        parameter.setDbAddresses(Arrays.asList(new InetSocketAddress(MYSQL_ADDRESS, 3306),
                new InetSocketAddress(MYSQL_ADDRESS, 3306)));
        parameter.setDbUsername(USERNAME);
        parameter.setDbPassword(PASSWORD);
        parameter.setPositions(Arrays.asList("{\"journalName\":\"\"," +
                "\"position\":\"\",\"timestamp\":\"\"}"
        ));

        // slaveId 小于等于0则随机生成
        parameter.setSlaveId(0L);

        parameter.setDefaultConnectionTimeoutInSeconds(30);
        parameter.setConnectionCharset("UTF-8");
        parameter.setConnectionCharsetNumber((byte) 33);
        parameter.setReceiveBufferSize(8 * 1024);
        parameter.setSendBufferSize(8 * 1024);

        parameter.setDetectingEnable(false);
        parameter.setDetectingIntervalInSeconds(10);
        parameter.setDetectingRetryTimes(3);
        parameter.setDetectingSQL(DETECTING_SQL);

        canal.setCanalParameter(parameter);
        return canal;
    }
}
