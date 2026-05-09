//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.debezium.connector.kb;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.kb.connection.KingbaseConnection;
import io.debezium.connector.kb.connection.KingbaseDefaultValueConverter;
import io.debezium.connector.kb.connection.ReplicaIdentityInfo;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class KingbaseSchema extends RelationalDatabaseSchema {
    //protected static final String PUBLIC_SCHEMA_NAME = "public";
    private static final Logger LOGGER = LoggerFactory.getLogger(KingbaseSchema.class);
    private final Map<TableId, List<String>> tableIdToToastableColumns = new HashMap();
    private final Map<Integer, TableId> relationIdToTableId = new HashMap();
    private final boolean readToastableColumns;

    protected KingbaseSchema(KingbaseConnectorConfig config, KingbaseDefaultValueConverter defaultValueConverter, TopicSelector<TableId> topicNamingStrategy, KingbaseValueConverter valueConverter) {
        super(config, topicNamingStrategy, config.getTableFilters().dataCollectionFilter(), config.getColumnFilter(), getTableSchemaBuilder(config, valueConverter, defaultValueConverter), false, config.getKeyMapper());
        this.readToastableColumns = config.skipRefreshSchemaOnMissingToastableData();
    }

    private static TableSchemaBuilder getTableSchemaBuilder(KingbaseConnectorConfig config, KingbaseValueConverter valueConverter, KingbaseDefaultValueConverter defaultValueConverter) {
        return new TableSchemaBuilder(valueConverter, defaultValueConverter, config.schemaNameAdjustmentMode().createAdjuster(), config.customConverterRegistry(), config.getSourceInfoStructMaker().schema(), config.getSanitizeFieldNames(), false);
    }

    protected KingbaseSchema refresh(KingbaseConnection connection, boolean printReplicaIdentityInfo) throws SQLException {
        connection.readSchema(this.tables(), (String)null, (String)null, this.getTableFilter(), (Tables.ColumnNameFilter)null, true);
        if (printReplicaIdentityInfo) {
            this.tableIds().forEach((tableId) -> this.printReplicaIdentityInfo(connection, tableId));
        }

        this.refreshSchemas();
        if (this.readToastableColumns) {
            this.tableIds().forEach((tableId) -> this.refreshToastableColumnsMap(connection, tableId));
        }

        return this;
    }

    private void printReplicaIdentityInfo(KingbaseConnection connection, TableId tableId) {
        try {
            ReplicaIdentityInfo replicaIdentity = connection.readReplicaIdentityInfo(tableId);
            LOGGER.info("REPLICA IDENTITY for '{}' is '{}'; {}", new Object[]{tableId, replicaIdentity, replicaIdentity.description()});
        } catch (SQLException var4) {
            LOGGER.warn("Cannot determine REPLICA IDENTITY info for '{}'", tableId);
        }

    }

    protected void refresh(KingbaseConnection connection, TableId tableId, boolean refreshToastableColumns) throws SQLException {
        Tables temp = new Tables();
        Objects.requireNonNull(tableId);
        connection.readSchema(temp, (String)null, (String)null, tableId::equals, (Tables.ColumnNameFilter)null, true);
        if (temp.size() == 0) {
            LOGGER.warn("Refresh of {} was requested but the table no longer exists", tableId);
        } else {
            this.tables().overwriteTable(temp.forTable(tableId));
            this.refreshSchema(tableId);
            if (refreshToastableColumns) {
                this.refreshToastableColumnsMap(connection, tableId);
            }

        }
    }

    public void refresh2(KingbaseConnection connection, TableId tableId, boolean refreshToastableColumns) throws SQLException {
        Tables temp = new Tables();
        String var10002 = tableId.catalog();
        String var10003 = tableId.schema();
        Objects.requireNonNull(tableId);
        connection.readSchema(temp, var10002, var10003, tableId::equals, (Tables.ColumnNameFilter)null, true);
        if (temp.size() == 0) {
            LOGGER.warn("Refresh of {} was requested but the table no longer exists", tableId);
        } else {
            this.tables().overwriteTable(temp.forTable(tableId));
            this.refreshSchema(tableId);
            if (refreshToastableColumns) {
                this.refreshToastableColumnsMap(connection, tableId);
            }

        }
    }

    protected boolean isFilteredOut(TableId id) {
        return !this.getTableFilter().isIncluded(id);
    }

    protected void refreshSchemas() {
        this.clearSchemas();
        this.tableIds().forEach(this::refreshSchema);
    }

    private void refreshToastableColumnsMap(KingbaseConnection connection, TableId tableId) {
        List<String> toastableColumns = new ArrayList();
        String relName = tableId.table();
        //修改: 不允许schema为空
        //String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        if(tableId.schema() == null || tableId.schema().isEmpty()) {
            throw new IllegalArgumentException("Schema can not be null or empty!");
        }
        String schema = tableId.schema();
        String statement = "select att.attname from pg_attribute att  join pg_class tbl on tbl.oid = att.attrelid join pg_namespace ns on tbl.relnamespace = ns.oid where tbl.relname = ? and ns.nspname = ? and att.attnum > 0 and att.attstorage in ('x', 'e', 'm') and not att.attisdropped;";

        try {
            connection.prepareQuery(statement, (stmt) -> {
                stmt.setString(1, relName);
                stmt.setString(2, schema);
            }, (rs) -> {
                while(rs.next()) {
                    toastableColumns.add(rs.getString(1));
                }

            });
            if (!connection.connection().getAutoCommit()) {
                connection.connection().commit();
            }
        } catch (SQLException e) {
            throw new ConnectException("Unable to refresh toastable columns mapping", e);
        }

        this.tableIdToToastableColumns.put(tableId, Collections.unmodifiableList(toastableColumns));
    }

    protected static TableId parse(String table) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            return null;
        } else {
            //修改: 不允许schema为空,
            if(tableId.schema() == null || tableId.schema().isEmpty()) {
                throw new IllegalArgumentException("Schema can not be null or empty!");
            }
            //return tableId.schema() == null ? new TableId(tableId.catalog(), "public", tableId.table()) : tableId;
            return tableId;
        }
    }

    public List<String> getToastableColumnsForTableId(TableId tableId) {
        return (List)this.tableIdToToastableColumns.getOrDefault(tableId, Collections.emptyList());
    }

    public void applySchemaChangesForTable(int relationId, Table table) {
        assert table != null;

        if (this.isFilteredOut(table.id())) {
            LOGGER.trace("Skipping schema refresh for table '{}' with relation '{}' as table is filtered", table.id(), relationId);
        } else {
            this.relationIdToTableId.put(relationId, table.id());
            this.refresh(table);
        }
    }

    public Table tableFor(int relationId) {
        TableId tableId = (TableId)this.relationIdToTableId.get(relationId);
        if (tableId == null) {
            LOGGER.debug("Relation '{}' is unknown, cannot resolve to table", relationId);
            return null;
        } else {
            LOGGER.debug("Relation '{}' resolved to table '{}'", relationId, tableId);
            return this.tableFor(tableId);
        }
    }

    public boolean tableInformationComplete() {
        return false;
    }
}
