/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * A PostgresSchema wrapper that routes partition table events to their parent tables.
 *
 * <p>For PostgreSQL 10+ partitioned tables, this class provides:
 *
 * <ul>
 *   <li>Child-to-parent routing: All partition child events are routed to parent table
 *   <li>Schema optimization: Loads one representative child per parent (to get primary keys)
 *   <li>Two-tier resolution: DB-derived mapping at startup, pattern matching for new partitions
 * </ul>
 *
 * <p>Note: In PostgreSQL 10, primary keys are defined on child partitions, not parent tables.
 * Therefore, we must load at least one child partition's schema to get the complete table
 * definition including primary keys.
 *
 * <p>This is particularly useful for PostgreSQL 10 where the 'publish_via_partition_root' parameter
 * is not available.
 */
public class PostgresPartitionRoutingSchema extends PostgresSchema {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionRoutingSchema.class);

    private final PostgresConnectorConfig dbzConfig;
    private final PostgresConnection jdbcConnection;
    private final boolean routeToParent;

    /** Parent tables parsed from {@code partition.tables} config. */
    private final Set<TableId> configuredParents;

    /** SQL to query one child for a given parent table. */
    private static final String SQL_QUERY_ONE_CHILD_FOR_PARENT =
            "SELECT cn.nspname AS child_schema, cc.relname AS child_table "
                    + "FROM pg_inherits "
                    + "JOIN pg_class cc ON inhrelid = cc.oid "
                    + "JOIN pg_namespace cn ON cc.relnamespace = cn.oid "
                    + "JOIN pg_class pc ON inhparent = pc.oid "
                    + "JOIN pg_namespace pn ON pc.relnamespace = pn.oid "
                    + "WHERE pn.nspname = ? AND pc.relname = ? "
                    + "LIMIT 1";

    /** Pattern-based router for fallback when child not in DB-derived mapping. */
    @Nullable private final PostgresPartitionRouter patternRouter;

    // ==================== Helper Methods ====================

    /** Checks if the given table is a configured partition parent. */
    private boolean isPartitionParent(TableId tableId) {
        return configuredParents.contains(tableId);
    }

    /**
     * Returns the data collection filter used for filtering tables.
     *
     * <p>This filter is consistent with the partition routing logic. WAL events from child tables
     * are routed to parent tables before filtering, so this filter only needs to include parent
     * tables.
     *
     * @return the table filter for data collection filtering
     */
    public Tables.TableFilter getDataCollectionFilter() {
        return this.getTableFilter();
    }

    public PostgresPartitionRoutingSchema(
            PostgresConnection jdbcConnection,
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter,
            PostgresPartitionRouter router)
            throws SQLException {
        super(
                new PostgresPartitionConnectorConfig(config.getConfig(), router),
                typeRegistry,
                jdbcConnection.getDefaultValueConverter(),
                topicSelector,
                valueConverter);
        this.dbzConfig = config;
        this.jdbcConnection = jdbcConnection;

        // Use router directly - all callers should provide a valid router
        this.patternRouter = router;
        this.routeToParent = router != null && router.isRoutingEnabled();

        // Get configuredParents from router's rules
        if (routeToParent && patternRouter != null) {
            this.configuredParents =
                    Collections.unmodifiableSet(patternRouter.getRules().getConfiguredParents());
        } else {
            this.configuredParents = Collections.emptySet();
        }

        LOG.info(
                "PostgresPartitionRoutingSchema initialized. routeToParent={}, db={}, configuredParents={}",
                this.routeToParent,
                config.databaseName(),
                this.configuredParents);
        // Load main tables and partition parents via super.refresh().
        // Partition child tables are excluded by PostgresPartitionConnectorConfig filter.
        refresh(jdbcConnection, false);
        if (routeToParent) {
            // Build child-to-parent mapping and fix parent table PKs from representative children
            preloadParentTables();
        }
    }

    @Override
    public Table tableFor(TableId id) {
        if (!routeToParent || id == null) {
            return super.tableFor(id);
        }
        return super.tableFor(resolveToParentOrSelf(id));
    }

    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        if (!routeToParent || table == null) {
            super.applySchemaChangesForTable(relationId, table);
            return;
        }
        TableId originalId = table.id();
        if (originalId == null) {
            super.applySchemaChangesForTable(relationId, table);
            return;
        }
        TableId parentId = resolveToParentOrSelf(originalId);
        if (!Objects.equals(originalId, parentId)) {
            Table rewritten = table.edit().tableId(parentId).create();
            super.applySchemaChangesForTable(relationId, rewritten);
        } else {
            super.applySchemaChangesForTable(relationId, table);
        }
    }

    @Override
    public io.debezium.relational.TableSchema schemaFor(TableId id) {
        if (!routeToParent || id == null) {
            return super.schemaFor(id);
        }
        return super.schemaFor(resolveToParentOrSelf(id));
    }

    /**
     * Resolves child table to parent, or returns itself if not a partition child.
     *
     * <p>Uses pattern matching via Router for resolution.
     */
    public TableId resolveToParentOrSelf(TableId tableId) {
        if (!routeToParent || tableId == null || patternRouter == null) {
            return tableId;
        }
        return patternRouter.route(tableId);
    }

    /**
     * Returns the partition router used for pattern-based routing.
     *
     * @return the partition router, or null if routing is disabled
     */
    @Nullable
    public PostgresPartitionRouter getPartitionRouter() {
        return patternRouter;
    }

    /**
     * Fixes parent table PKs from representative children.
     *
     * <p>PostgreSQL 10 defines PKs on child partitions, not parent tables.
     */
    private void preloadParentTables() throws SQLException {
        if (configuredParents.isEmpty()) {
            LOG.warn("No parent tables parsed from partition.tables config");
            return;
        }

        long start = System.currentTimeMillis();
        // Fix PKs for each parent from one representative child
        for (TableId parentId : configuredParents) {
            Table existingParent = tables().forTable(parentId);
            if (existingParent == null || !needsPrimaryKeyRefresh(parentId, existingParent)) {
                continue;
            }
            fixParentPrimaryKey(parentId);
        }

        LOG.info(
                "PostgresPartitionRoutingSchema preloaded {} parents in {} ms",
                configuredParents.size(),
                System.currentTimeMillis() - start);
    }

    /** Fix PK for a parent table by loading one child's schema. */
    private void fixParentPrimaryKey(TableId parentId) throws SQLException {
        TableId childId = queryOneChildForParent(parentId);
        if (childId == null) {
            LOG.debug("No child found for parent {}", parentId);
            return;
        }

        Tables childTables = new Tables();
        jdbcConnection.readSchema(
                childTables, dbzConfig.databaseName(), null, t -> t.equals(childId), null, false);

        Table childTable = childTables.forTable(childId);
        if (childTable != null) {
            Table parentWithPK = cloneTableWithId(childTable, parentId);
            tables().overwriteTable(parentWithPK);
            buildAndRegisterSchema(parentWithPK);
            LOG.debug("Fixed PK for parent {} from child {}", parentId, childId);
        }
    }

    /** Query one child for a given parent table. */
    private TableId queryOneChildForParent(TableId parentId) throws SQLException {
        final TableId[] childHolder = new TableId[1];
        jdbcConnection.prepareQuery(
                SQL_QUERY_ONE_CHILD_FOR_PARENT,
                st -> {
                    st.setString(1, parentId.schema());
                    st.setString(2, parentId.table());
                },
                rs -> {
                    if (rs.next()) {
                        childHolder[0] =
                                PostgresPartitionRules.createTableId(
                                        rs.getString("child_schema"), rs.getString("child_table"));
                    }
                });
        return childHolder[0];
    }

    private boolean needsPrimaryKeyRefresh(TableId resolvedId, Table existingTable) {
        if (existingTable == null) {
            return true;
        }
        // Only partition parents need PK fixup via representative child
        return isPartitionParent(resolvedId)
                && (existingTable.primaryKeyColumnNames() == null
                        || existingTable.primaryKeyColumnNames().isEmpty());
    }

    /**
     * Clones a table structure with a new table ID.
     *
     * <p>This is used to create partition child table definitions from parent table schemas,
     * allowing Debezium to process child partition events using the parent's schema.
     *
     * @param source the source table to clone
     * @param newId the new table ID for the cloned table
     * @return a new Table instance with the new ID and same schema as source
     */
    private Table cloneTableWithId(Table source, TableId newId) {
        TableEditor editor = Table.editor().tableId(newId);
        if (source.defaultCharsetName() != null) {
            editor.setDefaultCharsetName(source.defaultCharsetName());
        }
        for (io.debezium.relational.Column col : source.columns()) {
            editor.addColumn(col);
        }

        editor.setPrimaryKeyNames(source.primaryKeyColumnNames());
        return editor.create();
    }
}
