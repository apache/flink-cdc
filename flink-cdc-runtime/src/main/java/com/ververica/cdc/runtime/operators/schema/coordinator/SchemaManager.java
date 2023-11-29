/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.runtime.serializer.TableIdSerializer;
import com.ververica.cdc.runtime.serializer.schema.SchemaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.ververica.cdc.common.utils.Preconditions.checkArgument;

/**
 * Schema manager handles handles schema changes for tables, and manages historical schema versions
 * of tables.
 */
@Internal
public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);
    private static final int INITIAL_SCHEMA_VERSION = 0;
    private static final int VERSIONS_TO_KEEP = 3;

    // Serializer for checkpointing
    public static final Serializer SERIALIZER = new Serializer();

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> tableSchemas;

    public SchemaManager() {
        tableSchemas = new HashMap<>();
    }

    public SchemaManager(Map<TableId, SortedMap<Integer, Schema>> tableSchemas) {
        this.tableSchemas = tableSchemas;
    }

    /** Check if schema exists for the specified table ID. */
    public final boolean schemaExists(TableId tableId) {
        return tableSchemas.containsKey(tableId) && !tableSchemas.get(tableId).isEmpty();
    }

    /** Get the latest schema of the specified table. */
    public Optional<Schema> getLatestSchema(TableId tableId) {
        return getLatestSchemaVersion(tableId)
                .map(version -> tableSchemas.get(tableId).get(version));
    }

    /** Get schema at the specified version of a table. */
    public Schema getSchema(TableId tableId, int version) {
        checkArgument(
                tableSchemas.containsKey(tableId),
                "Unable to find schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = tableSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Apply schema change to a table. */
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            handleCreateTableEvent(((CreateTableEvent) schemaChangeEvent));
        } else {
            Optional<Schema> optionalSchema = getLatestSchema(schemaChangeEvent.tableId());
            checkArgument(
                    optionalSchema.isPresent(),
                    "Unable to apply SchemaChangeEvent for table \"%s\" without existing schema",
                    schemaChangeEvent.tableId());

            LOG.info("Handling schema change event: {}", schemaChangeEvent);
            registerNewSchema(
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
        return Objects.equals(tableSchemas, that.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableSchemas);
    }

    // -------------------------------- Helper functions -------------------------------------

    private Optional<Integer> getLatestSchemaVersion(TableId tableId) {
        if (!tableSchemas.containsKey(tableId)) {
            return Optional.empty();
        }
        try {
            return Optional.of(tableSchemas.get(tableId).lastKey());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    private void handleCreateTableEvent(CreateTableEvent event) {
        checkArgument(
                !schemaExists(event.tableId()),
                "Unable to apply CreateTableEvent to an existing schema for table \"%s\"",
                event.tableId());
        LOG.info("Handling schema change event: {}", event);
        registerNewSchema(event.tableId(), event.getSchema());
    }

    private void registerNewSchema(TableId tableId, Schema newSchema) {
        if (schemaExists(tableId)) {
            SortedMap<Integer, Schema> versionedSchemas = tableSchemas.get(tableId);
            Integer latestVersion = versionedSchemas.lastKey();
            versionedSchemas.put(latestVersion + 1, newSchema);
            if (versionedSchemas.size() > VERSIONS_TO_KEEP) {
                versionedSchemas.remove(versionedSchemas.firstKey());
            }
        } else {
            TreeMap<Integer, Schema> versionedSchemas = new TreeMap<>();
            versionedSchemas.put(INITIAL_SCHEMA_VERSION, newSchema);
            tableSchemas.putIfAbsent(tableId, versionedSchemas);
        }
    }

    /** Serializer for {@link SchemaManager}. */
    public static class Serializer implements SimpleVersionedSerializer<SchemaManager> {

        public static final int CURRENT_VERSION = 0;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(SchemaManager schemaManager) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                // Number of tables
                out.writeInt(schemaManager.tableSchemas.size());
                for (Map.Entry<TableId, SortedMap<Integer, Schema>> tableSchema :
                        schemaManager.tableSchemas.entrySet()) {
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
                return baos.toByteArray();
            }
        }

        @Override
        public SchemaManager deserialize(int version, byte[] serialized) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                // Total schema length
                int numTables = in.readInt();
                Map<TableId, SortedMap<Integer, Schema>> tableSchemas = new HashMap<>(numTables);
                for (int i = 0; i < numTables; i++) {
                    // Table ID
                    TableId tableId =
                            tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                    // Schema with versions
                    int numVersions = in.readInt();
                    SortedMap<Integer, Schema> versionedSchemas = new TreeMap<>(Integer::compareTo);
                    for (int j = 0; j < numVersions; j++) {
                        // Version
                        int schemaVersion = in.readInt();
                        Schema schema =
                                schemaSerializer.deserialize(new DataInputViewStreamWrapper(in));
                        versionedSchemas.put(schemaVersion, schema);
                    }
                    tableSchemas.put(tableId, versionedSchemas);
                }
                return new SchemaManager(tableSchemas);
            }
        }
    }
}
