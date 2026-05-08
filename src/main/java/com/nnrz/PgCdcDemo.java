package com.nnrz;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PgCdcDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // PG 数据源（核心）
        DebeziumSourceFunction<String> build = PostgreSQLSource.<String>builder()
                .hostname("192.168.2.101")
                .port(5432)
                .database("yjzhddxt_v3")
                .schemaList("yjzhddxtv3")
                .tableList("yjzhddxtv3.test_006")
                .username("postgres")
                .password("GXyjt@2021")
                .slotName("cdc_test")
                .decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env.addSource(build);

        streamSource.print();

        env.execute("PG CDC");
    }
}