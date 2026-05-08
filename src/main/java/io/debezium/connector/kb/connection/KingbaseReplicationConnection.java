//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.debezium.connector.kb.connection;

import com.kingbase8.core.BaseConnection;
import com.kingbase8.core.ServerVersion;
import com.kingbase8.replication.KBReplicationStream;
import com.kingbase8.replication.fluent.logical.ChainedLogicalStreamBuilder;
import com.kingbase8.util.KSQLException;
import com.kingbase8.util.KSQLState;
import io.debezium.DebeziumException;
import io.debezium.connector.kb.KingbaseConnectorConfig;
import io.debezium.connector.kb.KingbaseConnectorConfig.AutoCreateMode;
import io.debezium.connector.kb.KingbaseConnectorConfig.LogicalDecoder;
import io.debezium.connector.kb.KingbaseSchema;
import io.debezium.connector.kb.ReplicaIdentityMapper;
import io.debezium.connector.kb.TypeRegistry;
import io.debezium.connector.kb.connection.ReplicaIdentityInfo.ReplicaIdentity;
import io.debezium.connector.kb.connection.ServerInfo.ReplicationSlot;
import io.debezium.connector.kb.spi.SlotCreationResult;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnectionException;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KingbaseReplicationConnection extends JdbcConnection implements ReplicationConnection {
    private static final String SQL_STATE_INSUFFICIENT_PRIVILEGE = "42501";
    private static Logger LOGGER = LoggerFactory.getLogger(KingbaseReplicationConnection.class);
    private final String slotName;
    private final String publicationName;
    private final RelationalTableFilters tableFilter;
    private final AutoCreateMode publicationAutocreateMode;
    private final LogicalDecoder plugin;
    private final boolean dropSlotOnClose;
    private final KingbaseConnectorConfig connectorConfig;
    private final Duration statusUpdateInterval;
    private final MessageDecoder messageDecoder;
    private final KingbaseConnection jdbcConnection;
    private final TypeRegistry typeRegistry;
    private final Properties streamParams;
    private Lsn defaultStartingPos;
    private SlotCreationResult slotCreationInfo;
    private boolean hasInitedSlot;

    //对比源码, 这里进行了修改加上了Optional.empty()，否则会空指针
    private Optional<ReplicaIdentityMapper> replicaIdentityMapper = Optional.empty();


    private KingbaseReplicationConnection(KingbaseConnectorConfig config, String slotName, String publicationName, RelationalTableFilters tableFilter, AutoCreateMode publicationAutocreateMode, LogicalDecoder plugin, boolean dropSlotOnClose, Duration statusUpdateInterval, KingbaseConnection jdbcConnection, TypeRegistry typeRegistry, Properties streamParams, KingbaseSchema schema) {
        super(addDefaultSettings(config.getJdbcConfig()), KingbaseConnection.FACTORY, "\"", "\"");
        this.connectorConfig = config;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.tableFilter = tableFilter;
        this.publicationAutocreateMode = publicationAutocreateMode;
        this.plugin = plugin;
        this.dropSlotOnClose = dropSlotOnClose;
        this.statusUpdateInterval = statusUpdateInterval;
        this.messageDecoder = plugin.messageDecoder(new MessageDecoderContext(config, schema), jdbcConnection);
        this.jdbcConnection = jdbcConnection;
        this.typeRegistry = typeRegistry;
        this.streamParams = streamParams;
        this.slotCreationInfo = null;
        this.hasInitedSlot = false;
    }

    private static JdbcConfiguration addDefaultSettings(JdbcConfiguration configuration) {
        return JdbcConfiguration.adapt(KingbaseConnection.addDefaultSettings(configuration, "Debezium Streaming").edit().with("replication", "database").with("preferQueryMode", "simple").build());
    }

    private ServerInfo.ReplicationSlot getSlotInfo() throws SQLException, InterruptedException {
        KingbaseConnection connection = new KingbaseConnection(this.connectorConfig.getJdbcConfig(), "Debezium Slot Info");

        ServerInfo.ReplicationSlot var2;
        try {
            var2 = connection.readReplicationSlotInfo(this.slotName, this.plugin.getPostgresPluginName());
        } catch (Throwable var5) {
            try {
                connection.close();
            } catch (Throwable var4) {
                var5.addSuppressed(var4);
            }

            throw var5;
        }

        connection.close();
        return var2;
    }

    protected void initPublication() {
        String tableFilterString = null;
        if (LogicalDecoder.PGOUTPUT.equals(this.plugin)) {
            LOGGER.info("Initializing PgOutput logical decoder publication");

            try {
                Connection conn = this.pgConnection();
                conn.setAutoCommit(false);
                String selectPublication = String.format("SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s'", this.publicationName);
                Statement stmt = conn.createStatement();

                try {
                    ResultSet rs = stmt.executeQuery(selectPublication);

                    try {
                        if (rs.next()) {
                            Long count = rs.getLong(1);
                            if (count == 0L) {
                                LOGGER.info("Creating new publication '{}' for plugin '{}'", this.publicationName, this.plugin);
                                switch (this.publicationAutocreateMode) {
                                    case DISABLED:
                                        throw new ConnectException("Publication autocreation is disabled, please create one and restart the connector.");
                                    case ALL_TABLES:
                                        String createPublicationStmt = String.format("CREATE PUBLICATION %s FOR ALL TABLES;", this.publicationName);
                                        LOGGER.info("Creating Publication with statement '{}'", createPublicationStmt);
                                        stmt.execute(createPublicationStmt);
                                        break;
                                    case FILTERED:
                                        this.createOrUpdatePublicationModeFilterted(tableFilterString, stmt, false);
                                }
                            } else {
                                switch (this.publicationAutocreateMode) {
                                    case FILTERED:
                                        this.createOrUpdatePublicationModeFilterted(tableFilterString, stmt, true);
                                        break;
                                    default:
                                        LOGGER.trace("A logical publication named '{}' for plugin '{}' and database '{}' is already active on the server and will be used by the plugin", new Object[]{this.publicationName, this.plugin, this.database()});
                                }
                            }
                        }
                    } catch (Throwable var11) {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (Throwable var10) {
                                var11.addSuppressed(var10);
                            }
                        }

                        throw var11;
                    }

                    if (rs != null) {
                        rs.close();
                    }
                } catch (Throwable var12) {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (Throwable var9) {
                            var12.addSuppressed(var9);
                        }
                    }

                    throw var12;
                }

                if (stmt != null) {
                    stmt.close();
                }

                conn.commit();
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                throw new JdbcConnectionException(e);
            }
        }

    }

    private void createOrUpdatePublicationModeFilterted(String tableFilterString, Statement stmt, boolean isUpdate) {
        try {
            Set<TableId> tablesToCapture = this.determineCapturedTables();
            tableFilterString = (String)tablesToCapture.stream().map(TableId::toDoubleQuotedString).collect(Collectors.joining(", "));
            if (tableFilterString.isEmpty()) {
                throw new DebeziumException(String.format("No table filters found for filtered publication %s", this.publicationName));
            } else {
                String createOrUpdatePublicationStmt = isUpdate ? String.format("ALTER PUBLICATION %s SET TABLE %s;", this.publicationName, tableFilterString) : String.format("CREATE PUBLICATION %s FOR TABLE %s;", this.publicationName, tableFilterString);
                LOGGER.info(isUpdate ? "Updating Publication with statement '{}'" : "Creating Publication with statement '{}'", createOrUpdatePublicationStmt);
                stmt.execute(createOrUpdatePublicationStmt);
            }
        } catch (Exception e) {
            throw new ConnectException(String.format("Unable to %s filtered publication %s for %s", isUpdate ? "update" : "create", this.publicationName, tableFilterString), e);
        }
    }

    private void initReplicaIdentity() {
        if (this.replicaIdentityMapper.isPresent()) {
            LOGGER.info("Updating Replica Identity");

            Set<TableId> tablesCaptured;
            try {
                tablesCaptured = this.determineCapturedTables();
            } catch (Exception e) {
                throw new DebeziumException("Unable to get Captured tables", e);
            }

            tablesCaptured.forEach((tableId) -> {
                try {
                    Optional<ReplicaIdentityInfo> newReplicaIdentity = ((ReplicaIdentityMapper)this.replicaIdentityMapper.get()).findReplicaIdentity(tableId);
                    if (newReplicaIdentity.isPresent()) {
                        ReplicaIdentityInfo currentReplicaIdentity = null;

                        try {
                            currentReplicaIdentity = this.jdbcConnection.readReplicaIdentityInfo(tableId);
                            if (currentReplicaIdentity.getReplicaIdentity() == ReplicaIdentity.INDEX) {
                                currentReplicaIdentity.setIndexName(this.jdbcConnection.readIndexOfReplicaIdentity(tableId));
                            }
                        } catch (SQLException var5) {
                            LOGGER.error("Cannot determine REPLICA IDENTITY information for table {}", tableId);
                        }

                        if (currentReplicaIdentity != null && !currentReplicaIdentity.toString().equals(((ReplicaIdentityInfo)newReplicaIdentity.get()).toString())) {
                            this.jdbcConnection.setReplicaIdentityForTable(tableId, (ReplicaIdentityInfo)newReplicaIdentity.get());
                            LOGGER.info("Replica identity set to {} for table '{}'", newReplicaIdentity.get(), tableId);
                        } else {
                            LOGGER.info("Replica identity for table '{}' is already {}", tableId, currentReplicaIdentity);
                        }
                    } else {
                        LOGGER.debug("Replica identity for table '{}' will not be updated because Replica Identity is not defined on REPLICA_IDENTITY_AUTOSET_VALUES property", tableId);
                    }
                } catch (Exception e) {
                    LOGGER.error("Unable to update Replica Identity for table {}", tableId, e);
                }

            });
        }

    }

    private Set<TableId> determineCapturedTables() throws Exception {
        Set<TableId> allTableIds = this.jdbcConnection.getAllTableIds(this.connectorConfig.databaseName());
        Set<TableId> capturedTables = new HashSet();

        for(TableId tableId : allTableIds) {
            if (this.tableFilter.dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            } else {
                LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
            }
        }

        return (Set)capturedTables.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
    }

    protected void initReplicationSlot() throws SQLException, InterruptedException {
        ServerInfo.ReplicationSlot slotInfo = this.getSlotInfo();
        boolean shouldCreateSlot = ReplicationSlot.INVALID == slotInfo;

        try {
            if (shouldCreateSlot) {
                this.createReplicationSlot();
            }

            this.pgConnection();
            String identifySystemStatement = "IDENTIFY_SYSTEM";
            LOGGER.debug("running '{}' to validate replication connection", "IDENTIFY_SYSTEM");
            Lsn xlogStart = (Lsn)this.queryAndMap("IDENTIFY_SYSTEM", (rs) -> {
                if (!rs.next()) {
                    throw new IllegalStateException("The DB connection is not a valid replication connection");
                } else {
                    String xlogpos = rs.getString("xlogpos");
                    LOGGER.debug("received latest xlogpos '{}'", xlogpos);
                    return Lsn.valueOf(xlogpos);
                }
            });
            if (this.slotCreationInfo != null) {
                this.defaultStartingPos = this.slotCreationInfo.startLsn();
            } else if (!shouldCreateSlot && slotInfo.hasValidFlushedLsn()) {
                Lsn latestFlushedLsn = slotInfo.latestFlushedLsn();
                this.defaultStartingPos = latestFlushedLsn.compareTo(xlogStart) < 0 ? latestFlushedLsn : xlogStart;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("found previous flushed LSN '{}'", latestFlushedLsn);
                }
            } else {
                this.defaultStartingPos = xlogStart;
            }

            this.hasInitedSlot = true;
        } catch (SQLException e) {
            throw new JdbcConnectionException(e);
        }
    }

    private boolean useTemporarySlot() throws SQLException {
        return false;
    }

    public ReplicationStream startStreaming(WalPositionLocator walPosition) throws SQLException, InterruptedException {
        return this.startStreaming((Lsn)null, walPosition);
    }

    public ReplicationStream startStreaming(Lsn offset, WalPositionLocator walPosition) throws SQLException, InterruptedException {
        this.initConnection();
        this.connect();
        if (offset == null || !offset.isValid()) {
            offset = this.defaultStartingPos;
        }

        Lsn lsn = offset;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("starting streaming from LSN '{}'", offset);
        }

        int maxRetries = this.connectorConfig.maxRetries();
        Duration delay = this.connectorConfig.retryDelay();
        int tryCount = 0;

        while(true) {
            try {
                this.validateSlotIsInExpectedState(walPosition);
                return this.createReplicationStream(lsn, walPosition);
            } catch (Exception e) {
                String message = "Failed to start replication stream at " + lsn;
                ++tryCount;
                if (tryCount > maxRetries) {
                    if (e.getMessage().matches(".*replication slot .* is active.*")) {
                        message = message + "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                    }

                    throw new DebeziumException(message, e);
                }

                LOGGER.warn(message + ", waiting for {} ms and retrying, attempt number {} over {}", new Object[]{delay, tryCount, maxRetries});
                Metronome metronome = Metronome.sleeper(delay, Clock.SYSTEM);
                metronome.pause();
            }
        }
    }

    protected void validateSlotIsInExpectedState(WalPositionLocator walPosition) throws SQLException {
        Lsn lsn = walPosition.getLastCommitStoredLsn() != null ? walPosition.getLastCommitStoredLsn() : walPosition.getLastEventStoredLsn();
        if (lsn != null) {
            try {
                Statement stmt = this.pgConnection().createStatement();

                try {
                    String seekCommand = String.format("SELECT pg_replication_slot_advance('%s', '%s')", this.slotName, lsn.asString());
                    LOGGER.info("Seeking to {} on the replication slot with command {}", lsn, seekCommand);
                    stmt.execute(seekCommand);
                } catch (Throwable var7) {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (Throwable var6) {
                            var7.addSuppressed(var6);
                        }
                    }

                    throw var7;
                }

                if (stmt != null) {
                    stmt.close();
                }
            } catch (KSQLException var8) {
                if (!var8.getMessage().matches("ERROR: function pg_replication_slot_advance.*does not exist(.|\\n)*") && !KSQLState.UNDEFINED_FUNCTION.getState().equals(var8.getSQLState())) {
                    if (!var8.getMessage().matches("ERROR: must be superuser or replication role to use replication slots(.|\\n)*") && !"42501".equals(var8.getSQLState())) {
                        if (!var8.getMessage().matches("ERROR: cannot advance replication slot to.*") && !KSQLState.OBJECT_NOT_IN_STATE.getState().equals(var8.getSQLState())) {
                            switch (this.connectorConfig.getEventProcessingFailureHandlingMode()) {
                                case FAIL:
                                    throw new DebeziumException(var8);
                                case WARN:
                                    LOGGER.warn("Unexpected error while trying to seek LSN", var8);
                                    break;
                                case SKIP:
                                case IGNORE:
                                    LOGGER.debug("Unexpected error while trying to seek LSN", var8);
                            }
                        } else {
                            switch (this.connectorConfig.getEventProcessingFailureHandlingMode()) {
                                case FAIL:
                                case WARN:
                                    LOGGER.warn("Cannot seek to the last known offset '{}' on replication slot '{}'. Error from server: '{}'", new Object[]{lsn.asString(), this.slotName, var8.getMessage(), var8});
                                    break;
                                case SKIP:
                                case IGNORE:
                                    LOGGER.debug("Cannot seek to the last known offset '{}' on replication slot '{}'. Error from server: '{}'", new Object[]{lsn.asString(), this.slotName, var8.getMessage(), var8});
                            }
                        }
                    } else {
                        LOGGER.warn("Unable to use pg_replication_slot_advance() function. The Postgres server is likely on an old RDS version or privileges are not correctly set", var8);
                    }
                } else {
                    LOGGER.info("Postgres server doesn't support the command pg_replication_slot_advance(). Not seeking to last known offset.");
                }
            }

        }
    }

    public void initConnection() throws SQLException, InterruptedException {
        this.initPublication();
        this.initReplicaIdentity();
        if (!this.hasInitedSlot) {
            this.initReplicationSlot();
        }

    }

    public Optional<SlotCreationResult> createReplicationSlot() throws SQLException {
        LOGGER.debug("Creating new replication slot '{}' for plugin '{}'", this.slotName, this.plugin);
        String tempPart = "";
        boolean canExportSnapshot = this.pgConnection().haveMinimumServerVersion(ServerVersion.v9_4);
        if (this.dropSlotOnClose && !canExportSnapshot) {
            LOGGER.warn("A slot marked as temporary or with an exported snapshot was created, but not on a supported version of Postgres, ignoring!");
        }

        if (this.useTemporarySlot()) {
            tempPart = "TEMPORARY";
        }

        this.initPublication();
        Statement stmt = this.pgConnection().createStatement();

        Optional var5;
        try {
            String createCommand = String.format("CREATE_REPLICATION_SLOT \"%s\" %s LOGICAL %s", this.slotName, tempPart, this.plugin.getPostgresPluginName());
            LOGGER.info("Creating replication slot with command {}", createCommand);
            stmt.execute(createCommand);
            if (canExportSnapshot) {
                this.slotCreationInfo = this.parseSlotCreation(stmt.getResultSet());
            }

            var5 = Optional.ofNullable(this.slotCreationInfo);
        } catch (Throwable var7) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Throwable var6) {
                    var7.addSuppressed(var6);
                }
            }

            throw var7;
        }

        if (stmt != null) {
            stmt.close();
        }

        return var5;
    }

    protected BaseConnection pgConnection() throws SQLException {
        return (BaseConnection)this.connection(false);
    }

    private SlotCreationResult parseSlotCreation(ResultSet rs) {
        try {
            if (rs.next()) {
                String slotName = rs.getString("slot_name");
                String startPoint = rs.getString("consistent_point");
                String snapName = rs.getString("snapshot_name");
                String pluginName = rs.getString("output_plugin");
                return new SlotCreationResult(slotName, startPoint, snapName, pluginName);
            } else {
                throw new ConnectException("No replication slot found");
            }
        } catch (SQLException ex) {
            throw new ConnectException("Unable to parse create_replication_slot response", ex);
        }
    }

    private ReplicationStream createReplicationStream(final Lsn startLsn, final WalPositionLocator walPosition) throws SQLException, InterruptedException {
        final KBReplicationStream s;
        KBReplicationStream s1;
        try {
            try {
                MessageDecoder var7 = this.messageDecoder;
                Objects.requireNonNull(var7);
                s1 = this.startPgReplicationStream(startLsn, var7::defaultOptions);
            } catch (KSQLException e) {
                LOGGER.debug("Could not register for streaming, retrying without optional options", e);
                if (this.useTemporarySlot()) {
                    this.initReplicationSlot();
                }

                MessageDecoder var10002 = this.messageDecoder;
                Objects.requireNonNull(var10002);
                s1 = this.startPgReplicationStream(startLsn, var10002::defaultOptions);
            }
        } catch (KSQLException e) {
            if (e.getMessage().matches("(?s)ERROR: requested WAL segment .* has already been removed.*")) {
                LOGGER.error("Cannot rewind to last processed WAL position", e);
                throw new ConnectException("The offset to start reading from has been removed from the database write-ahead log. Create a new snapshot and consider setting of PostgreSQL parameter wal_keep_segments = 0.");
            }

            throw e;
        }

        s = s1;
        return new ReplicationStream() {
            private static final int CHECK_WARNINGS_AFTER_COUNT = 100;
            private int warningCheckCounter = 100;
            private ExecutorService keepAliveExecutor = null;
            private AtomicBoolean keepAliveRunning;
            private final Metronome metronome;
            private volatile Lsn lastReceivedLsn;

            {
                this.metronome = Metronome.sleeper(KingbaseReplicationConnection.this.statusUpdateInterval, Clock.SYSTEM);
            }

            public void read(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
                this.processWarnings(false);
                ByteBuffer read = s.read();
                Lsn lastReceiveLsn = Lsn.valueOf(s.getLastReceiveLSN());
                KingbaseReplicationConnection.LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startLsn, lastReceiveLsn);
                if (!KingbaseReplicationConnection.this.messageDecoder.shouldMessageBeSkipped(read, lastReceiveLsn, startLsn, walPosition)) {
                    this.deserializeMessages(read, processor);
                }
            }

            public boolean readPending(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
                this.processWarnings(false);
                ByteBuffer read = s.readPending();
                Lsn lastReceiveLsn = Lsn.valueOf(s.getLastReceiveLSN());
                KingbaseReplicationConnection.LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startLsn, lastReceiveLsn);
                if (read == null) {
                    return false;
                } else if (KingbaseReplicationConnection.this.messageDecoder.shouldMessageBeSkipped(read, lastReceiveLsn, startLsn, walPosition)) {
                    return true;
                } else {
                    this.deserializeMessages(read, processor);
                    return true;
                }
            }

            private void deserializeMessages(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
                this.lastReceivedLsn = Lsn.valueOf(s.getLastReceiveLSN());
                KingbaseReplicationConnection.LOGGER.trace("Received message at LSN {}", this.lastReceivedLsn);
                KingbaseReplicationConnection.this.messageDecoder.processMessage(buffer, processor, KingbaseReplicationConnection.this.typeRegistry);
            }

            public void close() throws SQLException {
                this.processWarnings(true);
                s.close();
            }

            public void flushLsn(Lsn lsn) throws SQLException {
                this.doFlushLsn(lsn);
            }

            private void doFlushLsn(Lsn lsn) throws SQLException {
                s.setFlushedLSN(lsn.asLogSequenceNumber());
                s.setAppliedLSN(lsn.asLogSequenceNumber());
                s.forceUpdateStatus();
            }

            public Lsn lastReceivedLsn() {
                return this.lastReceivedLsn;
            }

            public void startKeepAlive(ExecutorService service) {
                if (this.keepAliveExecutor == null) {
                    this.keepAliveExecutor = service;
                    this.keepAliveRunning = new AtomicBoolean(true);
                    this.keepAliveExecutor.submit(() -> {
                        while(this.keepAliveRunning.get()) {
                            try {
                                KingbaseReplicationConnection.LOGGER.trace("Forcing status update with replication stream");
                                s.forceUpdateStatus();
                                this.metronome.pause();
                            } catch (Exception exp) {
                                throw new RuntimeException("received unexpected exception will perform keep alive", exp);
                            }
                        }

                    });
                }

            }

            public void stopKeepAlive() {
                if (this.keepAliveExecutor != null) {
                    this.keepAliveRunning.set(false);
                    this.keepAliveExecutor.shutdownNow();
                    this.keepAliveExecutor = null;
                }

            }

            private void processWarnings(boolean forced) throws SQLException {
                if (--this.warningCheckCounter == 0 || forced) {
                    this.warningCheckCounter = 100;

                    for(SQLWarning w = KingbaseReplicationConnection.this.connection().getWarnings(); w != null; w = w.getNextWarning()) {
                        KingbaseReplicationConnection.LOGGER.debug("Server-side message: '{}', state = {}, code = {}", new Object[]{w.getMessage(), w.getSQLState(), w.getErrorCode()});
                    }

                    KingbaseReplicationConnection.this.connection().clearWarnings();
                }

            }

            public Lsn startLsn() {
                return startLsn;
            }
        };
    }

    private KBReplicationStream startPgReplicationStream(Lsn lsn, BiFunction<ChainedLogicalStreamBuilder, Function<Integer, Boolean>, ChainedLogicalStreamBuilder> configurator) throws SQLException {
        assert lsn != null;

        ChainedLogicalStreamBuilder streamBuilder = ((ChainedLogicalStreamBuilder)((ChainedLogicalStreamBuilder)this.pgConnection().getReplicationAPI().replicationStream().logical().withSlotName("\"" + this.slotName + "\"")).withStartPosition(lsn.asLogSequenceNumber())).withSlotOptions(this.streamParams);
        streamBuilder = (ChainedLogicalStreamBuilder)configurator.apply(streamBuilder, this::hasMinimumVersion);
        if (this.statusUpdateInterval != null && this.statusUpdateInterval.toMillis() > 0L) {
            streamBuilder.withStatusInterval(Math.toIntExact(this.statusUpdateInterval.toMillis()), TimeUnit.MILLISECONDS);
        }

        KBReplicationStream stream = streamBuilder.start();

        try {
            Thread.sleep(10L);
        } catch (Exception var6) {
        }

        stream.forceUpdateStatus();
        return stream;
    }

    private Boolean hasMinimumVersion(int version) {
        try {
            return this.pgConnection().haveMinimumServerVersion(version);
        } catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    public synchronized void close() {
        this.close(true);
    }

    public synchronized void close(boolean dropSlot) {
        try {
            LOGGER.debug("Closing message decoder");
            this.messageDecoder.close();
        } catch (Throwable e) {
            LOGGER.error("Unexpected error while closing message decoder", e);
        }

        try {
            LOGGER.debug("Closing replication connection");
            super.close();
        } catch (Throwable e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }

        if (this.dropSlotOnClose && dropSlot) {
            try {
                KingbaseConnection connection = new KingbaseConnection(this.connectorConfig.getJdbcConfig(), "Debezium Drop Slot");

                try {
                    connection.dropReplicationSlot(this.slotName);
                } catch (Throwable var6) {
                    try {
                        connection.close();
                    } catch (Throwable var5) {
                        var6.addSuppressed(var5);
                    }

                    throw var6;
                }

                connection.close();
            } catch (Throwable e) {
                LOGGER.error("Unexpected error while dropping replication slot", e);
            }
        }

    }

    public void reconnect() throws SQLException {
        this.close(false);
        this.connection(false);
    }

    protected static class ReplicationConnectionBuilder implements Builder {
        private final KingbaseConnectorConfig config;
        private String slotName = "debezium";
        private String publicationName = "dbz_publication";
        private RelationalTableFilters tableFilter;
        private AutoCreateMode publicationAutocreateMode;
        private LogicalDecoder plugin;
        private boolean dropSlotOnClose;
        private Duration statusUpdateIntervalVal;
        private TypeRegistry typeRegistry;
        private KingbaseSchema schema;
        private Properties slotStreamParams;
        private KingbaseConnection jdbcConnection;

        protected ReplicationConnectionBuilder(KingbaseConnectorConfig config) {
            this.publicationAutocreateMode = AutoCreateMode.ALL_TABLES;
            this.plugin = LogicalDecoder.DECODERBUFS;
            this.dropSlotOnClose = true;
            this.slotStreamParams = new Properties();

            assert config != null;

            this.config = config;
        }

        public ReplicationConnectionBuilder withSlot(String slotName) {
            assert slotName != null;

            this.slotName = slotName;
            return this;
        }

        public Builder withPublication(String publicationName) {
            assert publicationName != null;

            this.publicationName = publicationName;
            return this;
        }

        public Builder withTableFilter(RelationalTableFilters tableFilter) {
            assert tableFilter != null;

            this.tableFilter = tableFilter;
            return this;
        }

        public Builder withPublicationAutocreateMode(AutoCreateMode publicationAutocreateMode) {
            assert this.publicationName != null;

            this.publicationAutocreateMode = publicationAutocreateMode;
            return this;
        }

        public ReplicationConnectionBuilder withPlugin(LogicalDecoder plugin) {
            assert plugin != null;

            this.plugin = plugin;
            return this;
        }

        public ReplicationConnectionBuilder dropSlotOnClose(boolean dropSlotOnClose) {
            this.dropSlotOnClose = dropSlotOnClose;
            return this;
        }

        public ReplicationConnectionBuilder streamParams(String slotStreamParams) {
            if (slotStreamParams != null && !slotStreamParams.isEmpty()) {
                this.slotStreamParams = new Properties();
                String[] paramsWithValues = slotStreamParams.split(";");

                for(String paramsWithValue : paramsWithValues) {
                    String[] paramAndValue = paramsWithValue.split("=");
                    if (paramAndValue.length == 2) {
                        this.slotStreamParams.setProperty(paramAndValue[0], paramAndValue[1]);
                    } else {
                        KingbaseReplicationConnection.LOGGER.warn("The following STREAM_PARAMS value is invalid: {}", paramsWithValue);
                    }
                }
            }

            return this;
        }

        public ReplicationConnectionBuilder statusUpdateInterval(Duration statusUpdateInterval) {
            this.statusUpdateIntervalVal = statusUpdateInterval;
            return this;
        }

        public Builder jdbcMetadataConnection(KingbaseConnection jdbcConnection) {
            this.jdbcConnection = jdbcConnection;
            return this;
        }

        public ReplicationConnection build() {
            assert this.plugin != null : "Decoding plugin name is not set";

            return new KingbaseReplicationConnection(this.config, this.slotName, this.publicationName, this.tableFilter, this.publicationAutocreateMode, this.plugin, this.dropSlotOnClose, this.statusUpdateIntervalVal, this.jdbcConnection, this.typeRegistry, this.slotStreamParams, this.schema);
        }

        public Builder withTypeRegistry(TypeRegistry typeRegistry) {
            this.typeRegistry = typeRegistry;
            return this;
        }

        public Builder withSchema(KingbaseSchema schema) {
            this.schema = schema;
            return this;
        }
    }
}
