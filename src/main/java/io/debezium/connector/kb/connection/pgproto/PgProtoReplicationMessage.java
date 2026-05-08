//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.debezium.connector.kb.connection.pgproto;

import io.debezium.DebeziumException;
import io.debezium.connector.kb.KingbaseStreamingChangeEventSource;
import io.debezium.connector.kb.KingbaseType;
import io.debezium.connector.kb.TypeRegistry;
import io.debezium.connector.kb.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.kb.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.kb.connection.ReplicationMessage;
import io.debezium.connector.kb.connection.ReplicationMessageColumnValueResolver;
import io.debezium.connector.kb.proto.PgProto;
import io.debezium.connector.kb.proto.PgProto.Op;
import io.debezium.util.Strings;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PgProtoReplicationMessage implements ReplicationMessage {
    private final PgProto.RowMessage rawMessage;
    private final TypeRegistry typeRegistry;

    PgProtoReplicationMessage(PgProto.RowMessage rawMessage, TypeRegistry typeRegistry) {
        this.rawMessage = rawMessage;
        this.typeRegistry = typeRegistry;
        if(this.missingTypeMetadata()){
            throw new DebeziumException("Protobuf message does not contain metadata. Unsupported version of protobuf plug-in is deployed in the database.");
        }
    }

    public ReplicationMessage.Operation getOperation() {
        switch(this.rawMessage.getOp()){
            case INSERT:
                return Operation.INSERT;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            case BEGIN:
                return Operation.BEGIN;
            case COMMIT:
                return Operation.COMMIT;
            default:
                throw new IllegalArgumentException("Unknown operation '" + this.rawMessage.getOp() + "' in replication stream message");
        }
    }

    public Instant getCommitTime() {
        return Instant.ofEpochSecond(0L, this.rawMessage.getCommitTime() * 1000L);
    }

    public OptionalLong getTransactionId() {
        return OptionalLong.of(Integer.toUnsignedLong(this.rawMessage.getTransactionId()));
    }

    public String getTable() {
        return this.rawMessage.getTable();
    }

    //改动：添加一个获取模式的方法
    public String getSchema() {
        return this.rawMessage.getSchema();
    }

    public List<ReplicationMessage.Column> getOldTupleList() {
        return this.transform(this.rawMessage.getOldTupleList(), (List) null);
    }

    public List<ReplicationMessage.Column> getNewTupleList() {
        return this.transform(this.rawMessage.getNewTupleList(), this.rawMessage.getNewTypeinfoList());
    }

    private boolean missingTypeMetadata() {
        if(this.rawMessage.getOp() != Op.BEGIN && this.rawMessage.getOp() != Op.COMMIT && this.rawMessage.getOp() != Op.DELETE){
            return this.rawMessage.getNewTypeinfoList() == null;
        } else {
            return false;
        }
    }

    private List transform(List<PgProto.DatumMessage> messageList, List<PgProto.TypeInfo> typeInfoList) {
        return IntStream.range(0, messageList.size()).mapToObj((index) -> {
            final PgProto.DatumMessage datum = messageList.get(index);
            Optional<PgProto.TypeInfo> typeInfo = Optional.ofNullable(typeInfoList != null ? (PgProto.TypeInfo) typeInfoList.get(index) : null);
            final String columnName = Strings.unquoteIdentifierPart(datum.getColumnName());
            final KingbaseType type = this.typeRegistry.get(datum.getColumnType());
            if(datum.hasDatumMissing()){
                return new UnchangedToastedReplicationMessageColumn(columnName, type, typeInfo.map(PgProto.TypeInfo::getModifier).orElse(null), typeInfo.map(PgProto.TypeInfo::getValueOptional).orElse(Boolean.FALSE));
            } else {
                final String fullType = typeInfo.map(PgProto.TypeInfo::getModifier).orElse(null);
                return new AbstractReplicationMessageColumn(columnName, type, fullType, typeInfo.map(PgProto.TypeInfo::getValueOptional).orElse(Boolean.FALSE)) {
                    public Object getValue(KingbaseStreamingChangeEventSource.PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                        return PgProtoReplicationMessage.this.getValue(columnName, type, fullType, datum, connection, includeUnknownDatatypes);
                    }

                    public String toString() {
                        return datum.toString();
                    }
                };
            }
        }).collect(Collectors.toList());
    }

    public boolean isLastEventForLsn() {
        return true;
    }

    public Object getValue(String columnName, KingbaseType type, String fullType, PgProto.DatumMessage datumMessage, KingbaseStreamingChangeEventSource.PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
        PgProtoColumnValue columnValue = new PgProtoColumnValue(datumMessage);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, connection, includeUnknownDatatypes, this.typeRegistry);
    }

    public String toString() {
        return "PgProtoReplicationMessage [rawMessage=" + this.rawMessage + "]";
    }
}
