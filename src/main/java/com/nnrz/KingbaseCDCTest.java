package com.nnrz;

import com.nnrz.util.MyKafkaUtil;
import org.apache.flink.cdc.connectors.kb.KingbaseESSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class KingbaseCDCTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);


        Properties properties = new Properties();
        //快照模式(initial: 监听器启动的时候读取全表数据, never: 不做快照)
        properties.setProperty("snapshot.mode", "never");
        properties.setProperty("heartbeat.interval.ms", "60000");
        //服务名称需要唯一, 否则数据库复制槽需要几分钟才能变为active状态, 这里把库名拼接上是为了在KingbaseDeserializationSchema解析的时候可以获取到数据库名称
        properties.setProperty("database.server.name", String.format("kingbase_cdc_source_%s.%s", System.currentTimeMillis(), "yjzhddxt_v3"));

        SourceFunction<String> pgsqlSource = KingbaseESSource.<String>builder()
                .hostname("192.168.2.108")
                .port(54323)
                .database("test_db2")
                .schemaList("yjzhddxt_v3")
                .tableList("yjzhddxt_v3.*")
                .username("system")
                .password("123456")
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