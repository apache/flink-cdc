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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/**
 * Schema manager handles schema changes for tables, and manages historical schema versions of
 * tables.
 */
@Internal
public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);
    private static final int INITIAL_SCHEMA_VERSION = 0;
    private static final int VERSIONS_TO_KEEP = 3;
    private final SchemaChangeBehavior behavior;

    // Serializer for checkpointing
    public static final Serializer SERIALIZER = new Serializer();

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> originalSchemas;

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas;

    public SchemaManager() {
        evolvedSchemas = new HashMap<>();
        originalSchemas = new HashMap<>();
        behavior = SchemaChangeBehavior.EVOLVE;
    }

    public SchemaManager(SchemaChangeBehavior behavior) {
        evolvedSchemas = new HashMap<>();
        originalSchemas = new HashMap<>();
        this.behavior = behavior;
    }

    public SchemaManager(
            Map<TableId, SortedMap<Integer, Schema>> originalSchemas,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas,
            SchemaChangeBehavior behavior) {
        this.evolvedSchemas = evolvedSchemas;
        this.originalSchemas = originalSchemas;
        this.behavior = behavior;
    }

    public SchemaChangeBehavior getBehavior() {
        return behavior;
    }

    /**
     * This function checks if the given schema change event has been applied already. If so, it
     * will be ignored to avoid sending duplicate evolved schema change events to sink metadata
     * applier.
     */
    public final boolean isOriginalSchemaChangeEventRedundant(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Optional<Schema> latestSchema = getLatestOriginalSchema(tableId);
        return Boolean.TRUE.equals(
                SchemaChangeEventVisitor.visit(
                        event,
                        addColumnEvent -> {
                            // It has not been applied if schema does not even exist
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            List<Column> existedColumns = latestSchema.get().getColumns();

                            // It has been applied only if all columns are present in existedColumns
                            for (AddColumnEvent.ColumnWithPosition column :
                                    addColumnEvent.getAddedColumns()) {
                                if (!existedColumns.contains(column.getAddColumn())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        alterColumnTypeEvent -> {
                            // It has not been applied if schema does not even exist
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            Schema schema = latestSchema.get();

                            // It has been applied only if all column types are set as expected
                            for (Map.Entry<String, DataType> entry :
                                    alterColumnTypeEvent.getTypeMapping().entrySet()) {
                                if (!schema.getColumn(entry.getKey()).isPresent()
                                        || !schema.getColumn(entry.getKey())
                                                .get()
                                                .getType()
                                                .equals(entry.getValue())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        createTableEvent -> {
                            // It has been applied if such table already exists
                            return latestSchema.isPresent();
                        },
                        dropColumnEvent -> {
                            // It has not been applied if schema does not even exist
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            List<String> existedColumnNames = latestSchema.get().getColumnNames();

                            // It has been applied only if corresponding column types do not exist
                            return dropColumnEvent.getDroppedColumnNames().stream()
                                    .noneMatch(existedColumnNames::contains);
                        },
                        dropTableEvent -> {
                            // It has been applied if such table does not exist
                            return !latestSchema.isPresent();
                        },
                        renameColumnEvent -> {
                            // It has been applied if such table already exists
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            List<String> existedColumnNames = latestSchema.get().getColumnNames();

                            // It has been applied only if all previous names do not exist, and all
                            // new names already exist
                            for (Map.Entry<String, String> entry :
                                    renameColumnEvent.getNameMapping().entrySet()) {
                                if (existedColumnNames.contains(entry.getKey())
                                        || !existedColumnNames.contains(entry.getValue())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        truncateTableEvent -> {
                            // We have no way to ensure if a TruncateTableEvent has been applied
                            // before. Just assume it's not.
                            return false;
                        }));
    }

    public final boolean schemaExists(
            Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        return schemaMap.containsKey(tableId) && !schemaMap.get(tableId).isEmpty();
    }

    public final boolean originalSchemaExists(TableId tableId) {
        return schemaExists(originalSchemas, tableId);
    }

    public final boolean evolvedSchemaExists(TableId tableId) {
        return schemaExists(evolvedSchemas, tableId);
    }

    /** Get the latest evolved schema of the specified table. */
    public Optional<Schema> getLatestEvolvedSchema(TableId tableId) {
        return getLatestSchemaVersion(evolvedSchemas, tableId)
                .map(version -> evolvedSchemas.get(tableId).get(version));
    }

    /** Get the latest original schema of the specified table. */
    public Optional<Schema> getLatestOriginalSchema(TableId tableId) {
        return getLatestSchemaVersion(originalSchemas, tableId)
                .map(version -> originalSchemas.get(tableId).get(version));
    }

    /** Get schema at the specified version of a table. */
    public Schema getEvolvedSchema(TableId tableId, int version) {
        checkArgument(
                evolvedSchemas.containsKey(tableId),
                "Unable to find evolved schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = evolvedSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Get schema at the specified version of a table. */
    public Schema getOriginalSchema(TableId tableId, int version) {
        checkArgument(
                originalSchemas.containsKey(tableId),
                "Unable to find original schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = originalSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Apply schema change to a table. */
    public void applyOriginalSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            handleCreateTableEvent(originalSchemas, ((CreateTableEvent) schemaChangeEvent));
        } else {
            Optional<Schema> optionalSchema = getLatestOriginalSchema(schemaChangeEvent.tableId());
            checkArgument(
                    optionalSchema.isPresent(),
                    "Unable to apply SchemaChangeEvent for table \"%s\" without existing schema",
                    schemaChangeEvent.tableId());

            LOG.info("Handling original schema change event: {}", schemaChangeEvent);
            registerNewSchema(
                    originalSchemas,
                    schemaChangeEvent.tableId(),
                    SchemaUtils.applySchemaChangeEvent(optionalSchema.get(), schemaChangeEvent));
        }
    }

    /** Apply schema change to a table. */
    public void applyEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            handleCreateTableEvent(evolvedSchemas, ((CreateTableEvent) schemaChangeEvent));
        } else {
            Optional<Schema> optionalSchema = getLatestEvolvedSchema(schemaChangeEvent.tableId());
            checkArgument(
                    optionalSchema.isPresent(),
                    "Unable to apply SchemaChangeEvent for table \"%s\" without existing schema",
                    schemaChangeEvent.tableId());

            LOG.info("Handling evolved schema change event: {}", schemaChangeEvent);
            registerNewSchema(
                    evolvedSchemas,
                    schemaChangeEvent.tableId(),
                    SchemaUtils.applySchemaChangeEvent(optionalSchema.get(), schemaChangeEvent));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaManager that = (SchemaManager) o;
        return Objects.equals(originalSchemas, that.originalSchemas)
                && Objects.equals(evolvedSchemas, that.evolvedSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalSchemas, evolvedSchemas);
    }

    // -------------------------------- Helper functions -------------------------------------

    private Optional<Integer> getLatestSchemaVersion(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        if (!schemaMap.containsKey(tableId)) {
            return Optional.empty();
        }
        try {
            return Optional.of(schemaMap.get(tableId).lastKey());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    private void handleCreateTableEvent(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, CreateTableEvent event) {
        checkArgument(
                !schemaExists(schemaMap, event.tableId()),
                "Unable to apply CreateTableEvent to an existing schema for table \"%s\"",
                event.tableId());
        LOG.info("Handling schema change event: {}", event);
        registerNewSchema(schemaMap, event.tableId(), event.getSchema());
    }

    private void registerNewSchema(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            TableId tableId,
            Schema newSchema) {
        if (schemaExists(schemaMap, tableId)) {
            SortedMap<Integer, Schema> versionedSchemas = schemaMap.get(tableId);
            Integer latestVersion = versionedSchemas.lastKey();
            versionedSchemas.put(latestVersion + 1, newSchema);
            if (versionedSchemas.size() > VERSIONS_TO_KEEP) {
                versionedSchemas.remove(versionedSchemas.firstKey());
            }
        } else {
            TreeMap<Integer, Schema> versionedSchemas = new TreeMap<>();
            versionedSchemas.put(INITIAL_SCHEMA_VERSION, newSchema);
            schemaMap.putIfAbsent(tableId, versionedSchemas);
        }
    }

    /** Serializer for {@link SchemaManager}. */
    public static class Serializer implements SimpleVersionedSerializer<SchemaManager> {

        /**
         * Update history: from Version 3.0.0, set to 0, from version 3.1.1, updated to 1, from
         * version 3.2.0, updated to 2.
         */
        public static final int CURRENT_VERSION = 2;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(SchemaManager schemaManager) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                serializeSchemaMap(schemaManager.evolvedSchemas, out);
                serializeSchemaMap(schemaManager.originalSchemas, out);
                out.writeUTF(schemaManager.getBehavior().name());
                return baos.toByteArray();
            }
        }

        private static void serializeSchemaMap(
                Map<TableId, SortedMap<Integer, Schema>> schemaMap, DataOutputStream out)
                throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            // Number of tables
            out.writeInt(schemaMap.size());
            for (Map.Entry<TableId, SortedMap<Integer, Schema>> tableSchema :
                    schemaMap.entrySet()) {
                // Table ID
                TableId tableId = tableSchema.getKey();
                tableIdSerializer.serialize(tableId, new DataOutputViewStreamWrapper(out));

                // Schema with versions
                SortedMap<Integer, Schema> versionedSchemas = tableSchema.getValue();
                out.writeInt(versionedSchemas.size());
                for (Map.Entry<Integer, Schema> versionedSchema : versionedSchemas.entrySet()) {
                    // Version
                    Integer version = versionedSchema.getKey();
                    out.writeInt(version);
                    // Schema
                    Schema schema = versionedSchema.getValue();
                    schemaSerializer.serialize(schema, new DataOutputViewStreamWrapper(out));
                }
            }
        }

        @Override
        public SchemaManager deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                switch (version) {
                    case 0:
                    case 1:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> schemas =
                                    deserializeSchemaMap(version, in);
                            // In legacy mode, original schema and evolved schema never differs
                            return new SchemaManager(schemas, schemas, SchemaChangeBehavior.EVOLVE);
                        }
                    case 2:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas =
                                    deserializeSchemaMap(version, in);
                            Map<TableId, SortedMap<Integer, Schema>> originalSchemas =
                                    deserializeSchemaMap(version, in);
                            SchemaChangeBehavior behavior =
                                    SchemaChangeBehavior.valueOf(in.readUTF());
                            return new SchemaManager(originalSchemas, evolvedSchemas, behavior);
                        }
                    default:
                        throw new RuntimeException("Unknown serialize version: " + version);
                }
            }
        }

        private static Map<TableId, SortedMap<Integer, Schema>> deserializeSchemaMap(
                int version, DataInputStream in) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            // Total schema length
            int numTables = in.readInt();
            Map<TableId, SortedMap<Integer, Schema>> tableSchemas = new HashMap<>(numTables);
            for (int i = 0; i < numTables; i++) {
                // Table ID
                TableId tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                // Schema with versions
                int numVersions = in.readInt();
                SortedMap<Integer, Schema> versionedSchemas = new TreeMap<>(Integer::compareTo);
                for (int j = 0; j < numVersions; j++) {
                    // Version
                    int schemaVersion = in.readInt();
                    Schema schema =
                            schemaSerializer.deserialize(
                                    version, new DataInputViewStreamWrapper(in));
                    versionedSchemas.put(schemaVersion, schema);
                }
                tableSchemas.put(tableId, versionedSchemas);
            }
            return tableSchemas;
        }
    }
}
