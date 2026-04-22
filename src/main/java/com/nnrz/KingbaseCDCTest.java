package com.nnrz;

import com.nnrz.util.MyKafkaUtil;
import org.apache.flink.cdc.connectors.kb.KingbaseESSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.Properties;

public class KingbaseCDCTest {

    private static final long DEFAULT_HEARTBEAT_MS = Duration.ofMinutes(5).toMillis();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "initial");
        properties.setProperty("debezium.slot.drop.on.stop", "true");
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_MS));

        SourceFunction<String> pgsqlSource = KingbaseESSource.<String>builder()
                .hostname("192.168.2.108")
                .port(54323)
                .database("test_db")
                .schemaList("public")
                .tableList("public.*")
                .username("system")
                .password("test123")
                .deserializer(new KingbaseDeserializationSchema())
                .debeziumProperties(properties)
                //一个槽只能有一个监听程序, 如果需要多个监听器要创建不同的槽
                //.slotName("test_slot")
                .build();

        DataStreamSource<String> pgsqlDS = env.addSource(pgsqlSource);

        //将数据输出到kafka中
        pgsqlDS.addSink(MyKafkaUtil.getKafkaSink("kingbase_cdc_topic"));

        //打印到控制台
        pgsqlDS.print();

        //执行监听
        env.execute();
    }
}