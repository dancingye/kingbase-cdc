//package io.debezium.connector.kb;
//
//import io.debezium.connector.kb.connection.KingbaseConnection;
//import io.debezium.connector.kb.connection.Lsn;
//import io.debezium.connector.kb.spi.SlotCreationResult;
//import io.debezium.connector.kb.spi.SlotState;
//import io.debezium.connector.kb.spi.Snapshotter;
//import io.debezium.pipeline.EventDispatcher;
//import io.debezium.pipeline.source.spi.SnapshotProgressListener;
//import io.debezium.relational.RelationalSnapshotChangeEventSource;
//import io.debezium.relational.Table;
//import io.debezium.relational.TableId;
//import io.debezium.schema.SchemaChangeEvent;
//import io.debezium.util.Clock;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.SQLException;
//import java.time.Duration;
//import java.util.List;
//import java.util.Optional;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//public class KingbaseSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<KingbasePartition, KingbaseOffsetContext> {
//    private static final Logger LOGGER = LoggerFactory.getLogger(KingbaseSnapshotChangeEventSource.class);
//    private final KingbaseConnectorConfig connectorConfig;
//    private final KingbaseConnection jdbcConnection;
//    private final KingbaseSchema schema;
//    private final Snapshotter snapshotter;
//    private final SlotCreationResult slotCreatedInfo;
//    private final SlotState startingSlotInfo;
//
//    public KingbaseSnapshotChangeEventSource(KingbaseConnectorConfig connectorConfig, Snapshotter snapshotter, KingbaseConnection jdbcConnection, KingbaseSchema schema, EventDispatcher<KingbasePartition, TableId> dispatcher, Clock clock, SnapshotProgressListener<KingbasePartition> snapshotProgressListener, SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo) {
//        super(connectorConfig, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);
//        this.connectorConfig = connectorConfig;
//        this.jdbcConnection = jdbcConnection;
//        this.schema = schema;
//        this.snapshotter = snapshotter;
//        this.slotCreatedInfo = slotCreatedInfo;
//        this.startingSlotInfo = startingSlotInfo;
//    }
//
//    protected SnapshottingTask getSnapshottingTask(KingbasePartition partition, KingbaseOffsetContext previousOffset) {
//        boolean snapshotSchema = true;
//        boolean snapshotData = true;
//        snapshotData = this.snapshotter.shouldSnapshot();
//        if (snapshotData) {
//            LOGGER.info("According to the connector configuration data will be snapshotted");
//        } else {
//            LOGGER.info("According to the connector configuration no snapshot will be executed");
//            snapshotSchema = false;
//        }
//
//        return new SnapshottingTask(snapshotSchema, snapshotData);
//    }
//
//    protected SnapshotContext<KingbasePartition, KingbaseOffsetContext> prepare(KingbasePartition partition) throws Exception {
//        return new PostgresSnapshotContext(partition, this.connectorConfig.databaseName());
//    }
//
//    protected void connectionCreated(RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext) throws Exception {
//        if (this.snapshotter.shouldStreamEventsStartingFromSnapshot() && this.startingSlotInfo == null) {
//            this.setSnapshotTransactionIsolationLevel();
//        }
//
//        this.schema.refresh(this.jdbcConnection, false);
//    }
//
//    // ==============================================
//    // 🔥🔥🔥 修复：这里强制去掉 catalog，解决 NPE
//    // ==============================================
//    @Override
//    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> ctx) throws Exception {
//        Set<TableId> allTableIds = this.jdbcConnection.getAllTableIds(ctx.catalogName);
//
//        // 把带数据库名的 TableId 变成不带的，和内部存储一致
//        return allTableIds.stream()
//                .map(id -> new TableId(null, id.schema(), id.table()))
//                .collect(Collectors.toSet());
//    }
//
//    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext) throws SQLException, InterruptedException {
//        Duration lockTimeout = this.connectorConfig.snapshotLockTimeout();
//        Optional<String> lockStatement = this.snapshotter.snapshotTableLockingStatement(lockTimeout, snapshotContext.capturedTables);
//        if (lockStatement.isPresent()) {
//            LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
//            this.jdbcConnection.executeWithoutCommitting(new String[]{lockStatement.get()});
//            this.schema.refresh(this.jdbcConnection, false);
//        }
//    }
//
//    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext) throws SQLException {
//    }
//
//    protected void determineSnapshotOffset(RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> ctx, KingbaseOffsetContext previousOffset) throws Exception {
//        KingbaseOffsetContext offset = (KingbaseOffsetContext) ctx.offset;
//        if (offset == null) {
//            if (previousOffset != null && !this.snapshotter.shouldStreamEventsStartingFromSnapshot()) {
//                offset = KingbaseOffsetContext.initialContext(this.connectorConfig, this.jdbcConnection, this.getClock(), previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
//            } else {
//                offset = KingbaseOffsetContext.initialContext(this.connectorConfig, this.jdbcConnection, this.getClock());
//            }
//
//            ctx.offset = offset;
//        }
//
//        this.updateOffsetForSnapshot(offset);
//    }
//
//    private void updateOffsetForSnapshot(KingbaseOffsetContext offset) throws SQLException {
//        Lsn xlogStart = this.getTransactionStartLsn();
//        long txId = this.jdbcConnection.currentTransactionId();
//        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);
//        offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), this.clock.currentTime(), txId, offset.xmin(), null, null);
//    }
//
//    protected void updateOffsetForPreSnapshotCatchUpStreaming(KingbaseOffsetContext offset) throws SQLException {
//        this.updateOffsetForSnapshot(offset);
//        offset.setStreamingStoppingLsn(Lsn.valueOf(this.jdbcConnection.currentXLogLocation()));
//    }
//
//    private Lsn getTransactionStartLsn() throws SQLException {
//        if (this.slotCreatedInfo != null) {
//            return this.slotCreatedInfo.startLsn();
//        } else if (!this.snapshotter.shouldStreamEventsStartingFromSnapshot() && this.startingSlotInfo != null) {
//            SlotState currentSlotState = this.jdbcConnection.getReplicationSlotState(this.connectorConfig.slotName(), this.connectorConfig.plugin().getPostgresPluginName());
//            return currentSlotState.slotLastFlushedLsn();
//        } else {
//            return Lsn.valueOf(this.jdbcConnection.currentXLogLocation());
//        }
//    }
//
//    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext, KingbaseOffsetContext offsetContext) throws SQLException, InterruptedException {
//        for (String schema : (Set<String>) snapshotContext.capturedTables.stream().map(TableId::schema).collect(Collectors.toSet())) {
//            if (!sourceContext.isRunning()) {
//                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
//            }
//
//            LOGGER.info("Reading structure of schema '{}' of catalog '{}'", schema, snapshotContext.catalogName);
//            this.jdbcConnection.readSchema(snapshotContext.tables, snapshotContext.catalogName, schema, this.connectorConfig.getTableFilters().dataCollectionFilter(), null, false);
//        }
//
//        this.schema.refresh(this.jdbcConnection, false);
//    }
//
//    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext, Table table) throws SQLException {
//        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
//    }
//
//    protected void complete(SnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext) {
//        this.snapshotter.snapshotCompleted();
//    }
//
//    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> snapshotContext, TableId tableId, List<String> columns) {
//        return this.snapshotter.buildSnapshotQuery(tableId, columns);
//    }
//
//    protected void setSnapshotTransactionIsolationLevel() throws SQLException {
//        LOGGER.info("Setting isolation level");
//        String transactionStatement = this.snapshotter.snapshotTransactionIsolationLevelStatement(this.slotCreatedInfo);
//        LOGGER.info("Opening transaction with statement {}", transactionStatement);
//        this.jdbcConnection.executeWithoutCommitting(new String[]{transactionStatement});
//    }
//
//    private static class PostgresSnapshotContext extends RelationalSnapshotContext<KingbasePartition, KingbaseOffsetContext> {
//        PostgresSnapshotContext(KingbasePartition partition, String catalogName) throws SQLException {
//            super(partition, catalogName);
//        }
//    }
//}