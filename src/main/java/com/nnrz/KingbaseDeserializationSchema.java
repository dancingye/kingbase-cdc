package com.nnrz;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义解释格式
 */

public class KingbaseDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final Map<String, String> OP_MAP = new HashMap<String, String>() {{
        put("c", "I");
        put("u", "U");
        put("d", "D");
    }};

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {

        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String db = split[1];
        String schema = split[2];
        String tableName = split[3];

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

        //按照etl的格式, 添加额外字段
        afterJson.put("P_TAG_IUD", type); //操作类型
        afterJson.put("db", db); //数据库
        afterJson.put("schema", schema); //模式
        afterJson.put("table", tableName); //数据表

        // 从 Debezium 的 record key 中提取真实主键信息（支持联合主键）
        Object keyObj = sourceRecord.key();
        JSONObject primaryKeys = new JSONObject();
        if(keyObj instanceof Struct keyStruct){
            for(Field f : keyStruct.schema().fields()){
                primaryKeys.put(f.name(), keyStruct.get(f));
            }
        }
        afterJson.put("P_KEYS", primaryKeys);

        // 如果是删除, afterJson是空的, 需要从before中把真实主键值回填进去
        if("D".equals(type)){
            for(String keyName : primaryKeys.keySet()){
                afterJson.put(keyName, beforeJson.get(keyName));
            }
        }

        result.put("after", afterJson);

        collector.collect(afterJson.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}