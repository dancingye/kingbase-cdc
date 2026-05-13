package com.nnrz;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.nnrz.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.cdc.connectors.kb.KingbaseESSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class KingbaseCDCTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //快照间隔(如果监听器异常停止了下次启动的时候会从最后一次快照的地方开始监听, 需要保证中间不丢数据的话可以开启这个)
        //env.enableCheckpointing(30000);


        Properties properties = new Properties();
        //快照模式(initial: 监听器启动的时候读取全表数据, never: 不做快照)
        properties.setProperty("snapshot.mode", "never");
        properties.setProperty("heartbeat.interval.ms", "60000");
        //服务名称需要唯一, 否则数据库复制槽需要几分钟才能变为active状态, 这里把库名拼接上是为了在KingbaseDeserializationSchema解析的时候可以获取到数据库名称
        properties.setProperty("database.server.name", String.format("kingbase_cdc_source_%s.%s", System.currentTimeMillis(), "yjzhddxt_v3"));
        //自定数据解析, 这里是把时间戳微秒转成毫秒
        properties.setProperty("converters", "datetime");
        properties.setProperty("datetime.type", "com.nnrz.util.CustomDateTimeConverter");

        SourceFunction<String> pgsqlSource = KingbaseESSource.<String>builder()
                .hostname("192.168.2.108")
                .port(54323)
                .database("yjzhddxt_v3")
                .schemaList("yjzhddxt_v3")
                .tableList("yjzhddxt_v3.test_001", "yjzhddxt_v3.test_002")
                .username("system")
                .password("nnrz@5343885")
                .deserializer(new KingbaseDeserializationSchema())
                .debeziumProperties(properties)
                //一个槽只能有一个监听程序, 如果需要多个监听器要创建不同的槽
                .slotName(System.currentTimeMillis() + "")
                .build();

        DataStreamSource<String> pgsqlDS = env.addSource(pgsqlSource);

        //将数据输出到kafka中
        //pgsqlDS.addSink(MyKafkaUtil.getKafkaSink("kingbase_cdc_topic"));


        //按表名分组 → 同表批量聚合发送 Kafka
        pgsqlDS.keyBy(json -> {
                    // 按表名分组
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        return jsonObj.getString("table");
                    } catch (Exception e) {
                        return "unknown_table";
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(3)))
                .evictor(CountEvictor.of(200))
                .aggregate(new AggregateFunction<
                        String,
                        ArrayList<String>,
                        String>() {

                    @Override
                    public ArrayList<String> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public ArrayList<String> add(String value, ArrayList<String> acc) {
                        acc.add(value);
                        return acc;
                    }

                    @Override
                    public String getResult(ArrayList<String> acc) {
                        return "[" + String.join(",", acc) + "]";
                    }

                    @Override
                    public ArrayList<String> merge(ArrayList<String> a, ArrayList<String> b) {
                        a.addAll(b);
                        return a;
                    }
                })
                .addSink(MyKafkaUtil.getKafkaSink("kingbase_cdc_topic"));

        //打印到控制台
        pgsqlDS.print();

        //执行监听
        env.execute();
    }
}