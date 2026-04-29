package com.nnrz;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

public class KingbaseDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final Map<String, String> OP_MAP = new HashMap<String, String>() {{
        put("r", "read");
        put("c", "insert");
        put("u", "update");
        put("d", "delete");
    }};

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {

        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String schema = split[1];
        String tableName = split[2];

        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");

        JSONObject beforeJson = new JSONObject();
        if(before != null){
            for(Field f : before.schema().fields()){
                beforeJson.put(f.name(), before.get(f));
            }
        }

        JSONObject afterJson = new JSONObject();
        if(after != null){
            for(Field f : after.schema().fields()){
                afterJson.put(f.name(), after.get(f));
            }
        }

        String op = value.getString("op");
        String type = OP_MAP.getOrDefault(op, "unknown");

        result.put("schema", schema);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}