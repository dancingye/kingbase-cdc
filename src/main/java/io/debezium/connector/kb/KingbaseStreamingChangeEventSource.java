package io.debezium.connector.kb;

import com.kingbase8.core.BaseConnection;
import io.debezium.DebeziumException;
import io.debezium.connector.kb.connection.*;
import io.debezium.connector.kb.connection.ReplicationMessage.Operation;
import io.debezium.connector.kb.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

public class KingbaseStreamingChangeEventSource implements StreamingChangeEventSource<KingbasePartition, KingbaseOffsetContext> {
    private static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";
    private static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(KingbaseStreamingChangeEventSource.class);
    private static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;
    private final KingbaseConnection connection;
    private final KingbaseEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final KingbaseSchema schema;
    private final KingbaseConnectorConfig connectorConfig;
    private final KingbaseTaskContext taskContext;
    private final ReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference();
    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;
    private volatile boolean lsnFlushingAllowed = false;
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0L;
    private Lsn lastCompletelyProcessedLsn;
    private KingbaseOffsetContext effectiveOffset;

    public KingbaseStreamingChangeEventSource(KingbaseConnectorConfig connectorConfig, Snapshotter snapshotter, KingbaseConnection connection, KingbaseEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, KingbaseSchema schema, KingbaseTaskContext taskContext, ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval().toMillis());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());
    }

    public void init() {
        try {
            this.taskContext.refreshSchema(this.connection, true);
        } catch (SQLException e) {
            throw new DebeziumException("Error while executing initial schema load", e);
        }
    }

    private void initSchema() {
        try {
            this.taskContext.refreshSchema(this.connection, true);
        } catch (SQLException e) {
            throw new DebeziumException("Error while executing initial schema load", e);
        }
    }

    public void execute(ChangeEventSourceContext context, KingbasePartition partition, KingbaseOffsetContext offsetContext) throws InterruptedException {
        if (!this.snapshotter.shouldStream()) {
            LOGGER.info("Streaming is not enabled in correct configuration");
        } else {
            this.lsnFlushingAllowed = false;
            boolean hasStartLsnStoredInContext = offsetContext != null;

            // ==============================================
            // 🔥🔥🔥 关键修复：防止 effectiveOffset 为 null
            // ==============================================
            if (offsetContext == null) {
                LOGGER.warn("offsetContext is null, creating initial offset to avoid NPE");
                offsetContext = KingbaseOffsetContext.initialContext(connectorConfig, connection, clock);
            }
            this.effectiveOffset = offsetContext;

            try {
                WalPositionLocator walPosition;
                if (hasStartLsnStoredInContext) {
                    Lsn lsn = this.effectiveOffset.lastCompletelyProcessedLsn() != null ? this.effectiveOffset.lastCompletelyProcessedLsn() : this.effectiveOffset.lsn();
                    Operation lastProcessedMessageType = this.effectiveOffset.lastProcessedMessageType();
                    LOGGER.info("Retrieved latest position from stored offset '{}'", lsn);
                    walPosition = new WalPositionLocator(this.effectiveOffset.lastCommitLsn(), lsn, lastProcessedMessageType);
                    this.replicationStream.compareAndSet(null, this.replicationConnection.startStreaming(lsn, walPosition));
                } else {
                    LOGGER.info("No previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                    walPosition = new WalPositionLocator();
                    this.replicationStream.compareAndSet(null, this.replicationConnection.startStreaming(walPosition));
                }

                ReplicationStream stream = (ReplicationStream)this.replicationStream.get();
                stream.startKeepAlive(Threads.newSingleThreadExecutor(KingbaseConnector.class, this.connectorConfig.getLogicalName(), "keep-alive"));
                this.initSchema();
                if (!this.isInPreSnapshotCatchUpStreaming(this.effectiveOffset)) {
                    this.connection.commit();
                }

                this.lastCompletelyProcessedLsn = ((ReplicationStream)this.replicationStream.get()).startLsn();
                if (walPosition.searchingEnabled()) {
                    this.searchWalPosition(context, stream, walPosition);

                    try {
                        if (!this.isInPreSnapshotCatchUpStreaming(this.effectiveOffset)) {
                            this.connection.commit();
                        }
                    } catch (Exception e) {
                        LOGGER.info("Commit failed while preparing for reconnect", e);
                    }

                    walPosition.enableFiltering();
                    stream.stopKeepAlive();
                    this.replicationConnection.reconnect();
                    this.replicationStream.set(this.replicationConnection.startStreaming(walPosition.getLastEventStoredLsn(), walPosition));
                    stream = (ReplicationStream)this.replicationStream.get();
                    stream.startKeepAlive(Threads.newSingleThreadExecutor(KingbaseConnector.class, this.connectorConfig.getLogicalName(), "keep-alive"));
                }

                this.processMessages(context, partition, this.effectiveOffset, stream);
            } catch (Throwable e) {
                this.errorHandler.setProducerThrowable(e);
            } finally {
                if (this.replicationConnection != null) {
                    LOGGER.debug("stopping streaming...");
                    ReplicationStream stream = (ReplicationStream)this.replicationStream.get();
                    if (stream != null) {
                        stream.stopKeepAlive();
                    }

                    try {
                        if (!this.isInPreSnapshotCatchUpStreaming(offsetContext)) {
                            this.connection.commit();
                        }

                        this.replicationConnection.close();
                    } catch (Exception e) {
                        LOGGER.debug("Exception while closing the connection", e);
                    }

                    this.replicationStream.set(null);
                }

            }

        }
    }

    private void processMessages(ChangeEventSourceContext context, KingbasePartition partition, KingbaseOffsetContext offsetContext, ReplicationStream stream) throws SQLException, InterruptedException {
        LOGGER.info("Processing messages");
        int noMessageIterations = 0;

        while(context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null || this.lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0)) {
            boolean receivedMessage = stream.readPending((message) -> {
                Lsn lsn = stream.lastReceivedLsn();
                if (message.isLastEventForLsn()) {
                    this.lastCompletelyProcessedLsn = lsn;
                }

                if (message.isTransactionalMessage()) {
                    if (!this.connectorConfig.shouldProvideTransactionMetadata()) {
                        LOGGER.trace("Received transactional message {}", message);
                        if (message.getOperation() == Operation.COMMIT) {
                            this.commitMessage(partition, offsetContext, lsn);
                        }

                        return;
                    }

                    offsetContext.updateWalPosition(lsn, this.lastCompletelyProcessedLsn, message.getCommitTime(), this.toLong(message.getTransactionId()), this.taskContext.getSlotXmin(this.connection), (TableId)null, message.getOperation());
                    if (message.getOperation() == Operation.BEGIN) {
                        this.dispatcher.dispatchTransactionStartedEvent(partition, this.toString(message.getTransactionId()), offsetContext);
                    } else if (message.getOperation() == Operation.COMMIT) {
                        this.commitMessage(partition, offsetContext, lsn);
                        this.dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
                    }

                    this.maybeWarnAboutGrowingWalBacklog(true);
                } else if (message.getOperation() == Operation.MESSAGE) {
                    offsetContext.updateWalPosition(lsn, this.lastCompletelyProcessedLsn, message.getCommitTime(), this.toLong(message.getTransactionId()), this.taskContext.getSlotXmin(this.connection), message.getOperation());
                    if (message.isLastEventForLsn()) {
                        this.commitMessage(partition, offsetContext, lsn);
                    }

                    this.dispatcher.dispatchLogicalDecodingMessage(partition, offsetContext, this.clock.currentTimeAsInstant().toEpochMilli(), (LogicalDecodingMessage)message);
                    this.maybeWarnAboutGrowingWalBacklog(true);
                } else {
                    TableId tableId;
                    if (message.getOperation() != Operation.NOOP) {
                        tableId = KingbaseSchema.parse(message.getTable());
                        Objects.requireNonNull(tableId);
                    } else {
                        tableId = null;
                    }

                    offsetContext.updateWalPosition(lsn, this.lastCompletelyProcessedLsn, message.getCommitTime(), this.toLong(message.getTransactionId()), this.taskContext.getSlotXmin(this.connection), tableId, message.getOperation());
                    boolean dispatched = message.getOperation() != Operation.NOOP && this.dispatcher.dispatchDataChangeEvent(partition, tableId, new KingbaseChangeRecordEmitter(partition, offsetContext, this.clock, this.connectorConfig, this.schema, this.connection, tableId, message));
                    this.maybeWarnAboutGrowingWalBacklog(dispatched);
                }

            });
            this.probeConnectionIfNeeded();
            if (receivedMessage) {
                noMessageIterations = 0;
                this.lsnFlushingAllowed = true;
            } else {
                if (offsetContext.hasCompletelyProcessedPosition()) {
                    this.dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }

                ++noMessageIterations;
                if (noMessageIterations >= 5) {
                    noMessageIterations = 0;
                    this.pauseNoMessage.sleepWhen(true);
                }
            }

            if (!this.isInPreSnapshotCatchUpStreaming(offsetContext)) {
                this.connection.commit();
            }
        }

    }

    private void searchWalPosition(ChangeEventSourceContext context, ReplicationStream stream, WalPositionLocator walPosition) throws SQLException, InterruptedException {
        AtomicReference<Lsn> resumeLsn = new AtomicReference();
        int noMessageIterations = 0;
        LOGGER.info("Searching for WAL resume position");

        for(; context.isRunning() && resumeLsn.get() == null; this.probeConnectionIfNeeded()) {
            boolean receivedMessage = stream.readPending((message) -> {
                Lsn lsn = stream.lastReceivedLsn();
                resumeLsn.set((Lsn)walPosition.resumeFromLsn(lsn, message).orElse(null));
            });
            if (receivedMessage) {
                noMessageIterations = 0;
            } else {
                ++noMessageIterations;
                if (noMessageIterations >= 5) {
                    noMessageIterations = 0;
                    this.pauseNoMessage.sleepWhen(true);
                }
            }
        }

        LOGGER.info("WAL resume position '{}' discovered", resumeLsn.get());
    }

    private void probeConnectionIfNeeded() throws SQLException {
        if (this.connectionProbeTimer.hasElapsed()) {
            this.connection.prepareQuery("SELECT 1");
            this.connection.commit();
        }

    }

    private void commitMessage(KingbasePartition partition, KingbaseOffsetContext offsetContext, Lsn lsn) throws SQLException, InterruptedException {
        this.lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, this.lastCompletelyProcessedLsn);
        this.maybeWarnAboutGrowingWalBacklog(false);
        this.dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    private void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
        if (dispatched) {
            this.numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0L;
        } else {
            ++this.numberOfEventsSinceLastEventSentOrWalGrowingWarning;
        }

        if (this.numberOfEventsSinceLastEventSentOrWalGrowingWarning > 10000L && !this.dispatcher.heartbeatsEnabled()) {
            LOGGER.warn("Received {} events which were all filtered out, so no offset could be committed. This prevents the replication slot from acknowledging the processed WAL offsets, causing a growing backlog of non-removeable WAL segments on the database server. Consider to either adjust your filter configuration or enable heartbeat events (via the {} option) to avoid this situation.", this.numberOfEventsSinceLastEventSentOrWalGrowingWarning, "heartbeat.interval.ms");
            this.numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0L;
        }

    }

    public void commitOffset(Map<String, ?> offset) {
        try {
            ReplicationStream replicationStream = (ReplicationStream)this.replicationStream.get();
            Lsn commitLsn = Lsn.valueOf((Long)offset.get("lsn_commit"));
            Lsn changeLsn = Lsn.valueOf((Long)offset.get("lsn_proc"));
            Lsn lsn = commitLsn != null ? commitLsn : changeLsn;
            LOGGER.debug("Received offset commit request on commit LSN '{}' and change LSN '{}'", commitLsn, changeLsn);
            if (replicationStream != null && lsn != null) {
                if (!this.lsnFlushingAllowed) {
                    LOGGER.info("Received offset commit request on '{}', but ignoring it. LSN flushing is not allowed yet", lsn);
                    return;
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Flushing LSN to server: {}", lsn);
                }

                replicationStream.flushLsn(lsn);
            } else {
                LOGGER.debug("Streaming has already stopped, ignoring commit callback...");
            }

        } catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private boolean isInPreSnapshotCatchUpStreaming(KingbaseOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    private Long toLong(OptionalLong l) {
        return l.isPresent() ? l.getAsLong() : null;
    }

    private String toString(OptionalLong l) {
        return l.isPresent() ? String.valueOf(l.getAsLong()) : null;
    }

    @FunctionalInterface
    public interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }
}