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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Event deserializer for {@link PostgresDataSource}. */
@Internal
public class PostgresEventDeserializer extends DebeziumEventDeserializationSchema {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresEventDeserializer.class);
    private static final long serialVersionUID = 1L;
    private List<PostgreSQLReadableMetadata> readableMetadataList;
    private final boolean includeDatabaseInTableId;
    private final String databaseName;
    private Map<TableId, Schema> schemaMap = new HashMap<>();
    private final PostgresSourceConfig postgresSourceConfig;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Map<TableId, Map<String, Integer>> beforeTableColumnsOidMaps;

    public PostgresEventDeserializer(DebeziumChangelogMode changelogMode) {
        this(changelogMode, new ArrayList<>(), false, null, null, null);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList) {
        this(changelogMode, readableMetadataList, false, null, null, null);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList,
            boolean includeDatabaseInTableId) {
        this(changelogMode, readableMetadataList, includeDatabaseInTableId, null, null, null);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList,
            boolean includeDatabaseInTableId,
            String databaseName,
            PostgresSourceConfig postgresSourceConfig,
            Map<TableId, Map<String, Integer>> beforeTableColumnsOidMaps) {
        super(new PostgresSchemaDataTypeInference(), changelogMode);
        this.readableMetadataList = readableMetadataList;
        this.includeDatabaseInTableId = includeDatabaseInTableId;
        this.databaseName = databaseName;
        this.postgresSourceConfig = postgresSourceConfig;
        this.beforeTableColumnsOidMaps = beforeTableColumnsOidMaps;
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        return Collections.emptyList();
    }

    @Override
    public List<? extends Event> deserialize(SourceRecord record) throws Exception {
        List<Event> result = new ArrayList<>();
        if (postgresSourceConfig.isIncludeSchemaChanges()) {
            handleSchemaChange(record, result);
        }
        if (isDataChangeRecord(record)) {
            LOG.trace("Process data change record: {}", record);
            result.addAll(deserializeDataChangeRecord(record));
        } else if (isSchemaChangeRecord(record)) {
            LOG.trace("Process schema change record: {}", record);
        } else {
            LOG.trace("Ignored other record: {}", record);
            return Collections.emptyList();
        }
        return result;
    }

    private void handleSchemaChange(SourceRecord record, List<Event> result) {
        TableId tableId = getTableId(record);
        Schema valueSchema = record.valueSchema();
        Schema beforeSchema = schemaMap.get(tableId);
        List<String> beforeColumnNames;
        List<String> afterColumnNames;
        Schema afterSchema = fieldSchema(valueSchema, Envelope.FieldName.AFTER);
        List<Field> afterFields = afterSchema.fields();
        org.apache.flink.cdc.common.schema.Schema schema =
                PostgresSchemaUtils.getTableSchema(postgresSourceConfig, tableId);
        List<Column> columns = schema.getColumns();
        Map<String, Integer> beforeColumnsOidMaps =
                beforeTableColumnsOidMaps.get(TableId.tableId(tableId.getTableName()));
        // When the first piece of data arrives, beforeSchema is empty
        if (beforeSchema != null) {
            beforeColumnNames =
                    beforeSchema.fields().stream().map(e -> e.name()).collect(Collectors.toList());
            afterColumnNames = afterFields.stream().map(e -> e.name()).collect(Collectors.toList());
            List<String> newAddColumnNames = findAddedElements(beforeColumnNames, afterColumnNames);
            List<String> newDelColumnNames =
                    findRemovedElements(beforeColumnNames, afterColumnNames);
            Map<String, String> renameColumnMaps = new HashMap<>();
            // Process the fields of rename
            if (!newAddColumnNames.isEmpty() && !newDelColumnNames.isEmpty()) {
                renameColumnMaps =
                        getRenameColumnMaps(
                                tableId,
                                newAddColumnNames,
                                newDelColumnNames,
                                beforeSchema,
                                afterSchema,
                                beforeColumnsOidMaps);
                if (!renameColumnMaps.isEmpty()) {
                    result.add(new RenameColumnEvent(tableId, renameColumnMaps));
                    Map<String, String> finalRenameColumnMaps1 = renameColumnMaps;
                    renameColumnMaps
                            .keySet()
                            .forEach(
                                    e -> {
                                        beforeColumnsOidMaps.put(
                                                finalRenameColumnMaps1.get(e),
                                                beforeColumnsOidMaps.get(e));
                                    });
                }
                newAddColumnNames.removeAll(renameColumnMaps.values());
                newDelColumnNames.removeAll(renameColumnMaps.keySet());
            }
            // Process the fields of add
            if (!newAddColumnNames.isEmpty()) {
                newAddColumns(
                        tableId,
                        afterSchema,
                        result,
                        columns,
                        newAddColumnNames,
                        beforeColumnsOidMaps);
            }
            // Process the fields of delete
            if (!newDelColumnNames.isEmpty()) {
                newDelColumns(
                        tableId,
                        beforeColumnNames,
                        result,
                        newDelColumnNames,
                        beforeColumnsOidMaps);
            }
            // Handling fields with changed types
            findModifiedTypeColumns(
                    tableId,
                    beforeSchema,
                    beforeColumnNames,
                    afterSchema,
                    result,
                    columns,
                    renameColumnMaps);
        }
        schemaMap.put(tableId, afterSchema);
    }

    private void newDelColumns(
            TableId tableId,
            List<String> beforeColumnNames,
            List<Event> result,
            List<String> newDelColumnNames,
            Map<String, Integer> beforeColumnsOidMaps) {
        newDelColumnNames.forEach(
                e -> {
                    result.add(new DropColumnEvent(tableId, Collections.singletonList(e)));
                    beforeColumnNames.removeAll(newDelColumnNames);
                    beforeColumnsOidMaps.remove(e);
                });
    }

    private void newAddColumns(
            TableId tableId,
            Schema afterSchema,
            List<Event> result,
            List<Column> columns,
            List<String> newAddColumnNames,
            Map<String, Integer> beforeColumnsOidMaps) {
        List<Column> newAddColumns =
                columns.stream()
                        .filter(e -> newAddColumnNames.contains(e.getName()))
                        .collect(Collectors.toList());
        if (newAddColumns.size() != newAddColumnNames.size()) {
            List<String> notInCurrentTableColumns =
                    newAddColumnNames.stream()
                            .filter(e -> !newAddColumns.contains(e))
                            .collect(Collectors.toList());
            notInCurrentTableColumns.forEach(
                    e -> {
                        Field field = afterSchema.field(e);
                        DataType dataType = convertKafkaTypeToFlintType(field.schema());
                        PhysicalColumn column =
                                new PhysicalColumn(e, dataType, field.schema().doc());
                        result.add(
                                new AddColumnEvent(
                                        tableId,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        column,
                                                        AddColumnEvent.ColumnPosition.LAST,
                                                        null))));
                    });
        }
        newAddColumns.forEach(
                e -> {
                    SchemaChangeEvent event =
                            new AddColumnEvent(
                                    tableId,
                                    Collections.singletonList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    e, AddColumnEvent.ColumnPosition.LAST, null)));
                    result.add(event);
                });
        Map<String, Integer> newAddColumnsOidMaps =
                PostgresSchemaUtils.getColumnOids(postgresSourceConfig, tableId, newAddColumnNames);
        newAddColumnNames.forEach(
                e -> {
                    beforeColumnsOidMaps.put(e, newAddColumnsOidMaps.getOrDefault(e, null));
                });
    }

    private static DataType convertKafkaTypeToFlintType(Schema kafkaSchema) {
        switch (kafkaSchema.type()) {
            case STRING:
                return DataTypes.STRING();
            case INT8:
                return DataTypes.TINYINT();
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case BYTES:
                return DataTypes.BYTES();
            case ARRAY:
                Schema elementSchema = kafkaSchema.valueSchema();
                DataType elementType = convertKafkaTypeToFlintType(elementSchema);
                return DataTypes.ARRAY(elementType);
            case STRUCT:
                Schema schema = kafkaSchema.valueSchema();
                DataType dataType = convertKafkaTypeToFlintType(schema);
                return DataTypes.ROW(dataType);
            case MAP:
                return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
            default:
                return DataTypes.STRING();
        }
    }

    private void findModifiedTypeColumns(
            TableId tableId,
            Schema oldSchema,
            List<String> oldColumnNames,
            Schema afterSchema,
            List<Event> result,
            List<Column> columns,
            Map<String, String> renameColumnMaps) {
        Map<String, String> finalRenameColumnMaps = renameColumnMaps;
        oldColumnNames.stream()
                .forEach(
                        oldFieldName -> {
                            Field oldField = oldSchema.field(oldFieldName);
                            Field afterField = afterSchema.field(oldFieldName);
                            if (afterField == null) {
                                afterField =
                                        afterSchema.field(finalRenameColumnMaps.get(oldFieldName));
                            }
                            String afterFieldName = afterField.name();
                            if (!oldField.schema()
                                            .type()
                                            .getName()
                                            .equals(afterField.schema().type().getName())
                                    || (oldField.schema().defaultValue() != null
                                            && afterField.schema().defaultValue() != null
                                            && !oldField.schema()
                                                    .defaultValue()
                                                    .equals(afterField.schema().defaultValue()))
                                    || (oldField.schema().parameters() != null
                                            && afterField.schema().parameters() != null
                                            && !oldField.schema()
                                                    .parameters()
                                                    .equals(afterField.schema().parameters()))) {
                                Map<String, DataType> typeMapping = new HashMap<>();
                                Column column =
                                        columns.stream()
                                                .filter(e -> e.getName().equals(afterFieldName))
                                                .findFirst()
                                                .get();
                                typeMapping.put(afterField.name(), column.getType());
                                result.add(new AlterColumnTypeEvent(tableId, typeMapping));
                            }
                        });
    }

    private Map<String, String> getRenameColumnMaps(
            TableId tableId,
            List<String> newAddColumnNames,
            List<String> newDelColumnNames,
            Schema oldSchema,
            Schema afterSchema,
            Map<String, Integer> beforeColumnsOidMaps) {
        Map<String, String> renameColumnMaps = new HashMap<>();
        Map<String, Integer> newAddColumnsOidMaps =
                PostgresSchemaUtils.getColumnOids(postgresSourceConfig, tableId, newAddColumnNames);
        Map<String, Integer> newDelColumnsOidMaps = new HashMap<>();

        newDelColumnNames.forEach(
                e -> {
                    if (beforeColumnsOidMaps.keySet().contains(e)) {
                        newDelColumnsOidMaps.put(e, beforeColumnsOidMaps.get(e));
                    } else {
                        newDelColumnsOidMaps.put(e, null);
                    }
                });

        newAddColumnNames.forEach(
                e -> {
                    if (!newAddColumnsOidMaps.keySet().contains(e)) {
                        newAddColumnsOidMaps.put(e, null);
                    }
                });
        for (Map.Entry<String, Integer> newDelEntry : newDelColumnsOidMaps.entrySet()) {
            for (Map.Entry<String, Integer> newAddEntry : newAddColumnsOidMaps.entrySet()) {
                if ((newDelEntry.getValue() == null && newAddEntry.getValue() == null)
                        || (newDelEntry.getValue() == null && newAddEntry.getValue() != null)) {
                    int oldFieldIndex = oldSchema.field(newDelEntry.getKey()).index();
                    int afterFieldIndex = afterSchema.field(newAddEntry.getKey()).index();
                    if (oldFieldIndex == afterFieldIndex) {
                        renameColumnMaps.put(newDelEntry.getKey(), newAddEntry.getKey());
                    }
                } else if (newDelEntry.getValue().equals(newAddEntry.getValue())) {
                    renameColumnMaps.put(newDelEntry.getKey(), newAddEntry.getKey());
                }
            }
        }
        return renameColumnMaps;
    }

    public static List<String> findAddedElements(List<String> a, List<String> b) {
        Set<String> setA = new HashSet<>(a);
        Set<String> setB = new HashSet<>(b);
        setB.removeAll(setA);
        return new ArrayList<>(setB);
    }

    public static List<String> findRemovedElements(List<String> a, List<String> b) {
        Set<String> setA = new HashSet<>(a);
        Set<String> setB = new HashSet<>(b);
        setA.removeAll(setB);
        return new ArrayList<>(setA);
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        return false;
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        String[] parts = record.topic().split("\\.");
        if (includeDatabaseInTableId && databaseName != null) {
            return TableId.tableId(databaseName, parts[1], parts[2]);
        } else {
            return TableId.tableId(parts[1], parts[2]);
        }
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        readableMetadataList.forEach(
                (postgresReadableMetadata -> {
                    Object metadata = postgresReadableMetadata.getConverter().read(record);
                    if (postgresReadableMetadata.equals(PostgreSQLReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                postgresReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(
                                postgresReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }

    @Override
    protected Object convertToString(Object dbzObj, Schema schema) {
        // the Geometry datatype in PostgreSQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())
                || Geography.LOGICAL_NAME.equals(schema.name())) {
            try {
                Struct geometryStruct = (Struct) dbzObj;
                byte[] wkb = geometryStruct.getBytes("wkb");

                WKBReader wkbReader = new WKBReader();
                org.locationtech.jts.geom.Geometry jtsGeom = wkbReader.read(wkb);

                Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
                Map<String, Object> geometryInfo = new HashMap<>();
                String geometryType = jtsGeom.getGeometryType();
                geometryInfo.put("type", geometryType);

                if (geometryType.equals("GeometryCollection")) {
                    geometryInfo.put("geometries", jtsGeom.toText());
                } else {
                    Coordinate[] coordinates = jtsGeom.getCoordinates();
                    List<double[]> coordinateList = new ArrayList<>();
                    if (coordinates != null) {
                        for (Coordinate coordinate : coordinates) {
                            coordinateList.add(new double[] {coordinate.x, coordinate.y});
                            geometryInfo.put(
                                    "coordinates", new double[] {coordinate.x, coordinate.y});
                        }
                    }
                    geometryInfo.put(
                            "coordinates", OBJECT_MAPPER.writeValueAsString(coordinateList));
                }
                geometryInfo.put("srid", srid.orElse(0));
                return BinaryStringData.fromString(OBJECT_MAPPER.writeValueAsString(geometryInfo));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", dbzObj), e);
            }
        } else {
            return BinaryStringData.fromString(dbzObj.toString());
        }
    }
}
