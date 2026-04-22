package io.debezium.connector.kb;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.kb.SourceInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class KingbaseSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {
    private final Schema schema;

    public KingbaseSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        this.schema = this.commonSchemaBuilder()
                .name("io.debezium.connector.kb.Source")
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
                .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("xmin", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return this.schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        assert sourceInfo.database() != null && sourceInfo.schemaName() != null && sourceInfo.tableName() != null;

        Struct result = super.commonStruct(sourceInfo);
        result.put("schema", sourceInfo.schemaName());
        result.put("table", sourceInfo.tableName());

        // ===================== 修复点：唯一改动 =====================
        // 原来：sourceInfo.snapshot() → 报错 protected 无法访问
        // 修复：用 isSnapshot() 这个 public 方法等价判断
        if (!sourceInfo.isSnapshot()) {
            // ==========================================================
            if (sourceInfo.txId() != null) {
                result.put("txId", sourceInfo.txId());
            }

            if (sourceInfo.lsn() != null) {
                result.put("lsn", sourceInfo.lsn().asLong());
            }

            if (sourceInfo.xmin() != null) {
                result.put("xmin", sourceInfo.xmin());
            }
        }

        return result;
    }
}