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

package org.apache.flink.cdc.connectors.milvus.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSink;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusCollectionUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusNameUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;
import org.apache.flink.table.api.ValidationException;

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.COLLECTION_COLLISION_CHECK_TABLES;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.COLLECTION_MAPPING;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.COLLECTION_NAME_NORMALIZE_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.CONSISTENCY_LEVEL;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.CREATE_COLLECTION_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.CREATE_INDEX_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.DATABASE_NAME;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.ENABLE_DYNAMIC_FIELD;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.INDEX_METRIC_TYPE;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.INDEX_PARAMS_PREFIX;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.INDEX_TYPE;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.LOAD_COLLECTION_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.PARTITION_AUTO_CREATE_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.PARTITION_AUTO_CREATE_MAX_COUNT;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.PARTITION_FIELD;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.PARTITION_NAMES;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.PRIMARY_KEY_FIELD;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.RPC_DEADLINE;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_ADAPTIVE_BATCH_SPLIT_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_ADAPTIVE_BATCH_SPLIT_MIN_ROWS;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_ALLOW_NO_PRIMARY_KEY;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_DELETE_ENABLED;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_FLUSH_MAX_ROWS;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_PRIMARY_KEY_CHANGE_MODE;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.SINK_RETRY_BACKOFF;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.TOKEN;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.URI;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.VARCHAR_MAX_LENGTH_DEFAULT;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.VECTOR_FIELDS;
import static org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkOptions.VECTOR_FIELDS_TABLE_PREFIX;

/** Factory for creating configured {@link MilvusDataSink} instances. */
@Internal
public class MilvusDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "milvus";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(VECTOR_FIELDS_TABLE_PREFIX, INDEX_PARAMS_PREFIX);

        Configuration config = context.getFactoryConfiguration();
        validatePositive(config.get(SINK_FLUSH_MAX_ROWS), SINK_FLUSH_MAX_ROWS.key());
        validatePositive(config.get(VARCHAR_MAX_LENGTH_DEFAULT), VARCHAR_MAX_LENGTH_DEFAULT.key());
        validateNonNegative(config.get(SINK_MAX_RETRIES), SINK_MAX_RETRIES.key());
        validatePositiveDuration(config.get(SINK_FLUSH_INTERVAL), SINK_FLUSH_INTERVAL.key());
        validateNonNegativeDuration(config.get(SINK_RETRY_BACKOFF), SINK_RETRY_BACKOFF.key());
        validatePositiveDuration(config.get(CONNECT_TIMEOUT), CONNECT_TIMEOUT.key());
        validatePositiveDuration(config.get(RPC_DEADLINE), RPC_DEADLINE.key());
        validatePositive(
                config.get(SINK_ADAPTIVE_BATCH_SPLIT_MIN_ROWS),
                SINK_ADAPTIVE_BATCH_SPLIT_MIN_ROWS.key());
        validatePositive(
                config.get(PARTITION_AUTO_CREATE_MAX_COUNT), PARTITION_AUTO_CREATE_MAX_COUNT.key());
        validatePrimaryKeyChangeMode(config.get(SINK_PRIMARY_KEY_CHANGE_MODE));
        if (config.get(PARTITION_FIELD) != null && !config.get(PARTITION_FIELD).trim().isEmpty()) {
            MilvusNameUtils.validateIdentifier(
                    config.get(PARTITION_FIELD).trim(), "partition field");
        }
        validateEnum(IndexParam.IndexType.class, config.get(INDEX_TYPE), INDEX_TYPE.key());
        validateEnum(
                IndexParam.MetricType.class,
                config.get(INDEX_METRIC_TYPE),
                INDEX_METRIC_TYPE.key());
        if (config.get(SINK_ALLOW_NO_PRIMARY_KEY)) {
            throw new ValidationException(
                    SINK_ALLOW_NO_PRIMARY_KEY.key()
                            + " is not supported because Milvus CDC writes require deterministic primary keys.");
        }
        if (config.get(CONSISTENCY_LEVEL) != null && !config.get(CONSISTENCY_LEVEL).isEmpty()) {
            validateEnum(
                    ConsistencyLevel.class, config.get(CONSISTENCY_LEVEL), CONSISTENCY_LEVEL.key());
        }

        List<MilvusVectorFieldSpec> vectorFields =
                parseVectorFields(config.get(VECTOR_FIELDS), VECTOR_FIELDS.key());
        Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields =
                parseTableVectorFields(config);
        List<String> partitionNames = parsePartitionNames(config.get(PARTITION_NAMES));

        MilvusDataSinkConfig sinkConfig =
                new MilvusDataSinkConfig(
                        config.get(URI),
                        config.get(TOKEN),
                        config.get(CONNECT_TIMEOUT),
                        config.get(RPC_DEADLINE),
                        config.get(DATABASE_NAME),
                        parseCollectionMappings(config.get(COLLECTION_MAPPING)),
                        config.get(COLLECTION_NAME_NORMALIZE_ENABLED),
                        config.get(CREATE_COLLECTION_ENABLED),
                        config.get(CREATE_INDEX_ENABLED),
                        config.get(ENABLE_DYNAMIC_FIELD),
                        config.get(LOAD_COLLECTION_ENABLED),
                        config.get(PARTITION_FIELD),
                        config.get(PARTITION_AUTO_CREATE_ENABLED),
                        config.get(PARTITION_AUTO_CREATE_MAX_COUNT),
                        partitionNames,
                        vectorFields,
                        tableVectorFields,
                        config.get(PRIMARY_KEY_FIELD),
                        config.get(VARCHAR_MAX_LENGTH_DEFAULT),
                        config.get(SINK_FLUSH_MAX_ROWS),
                        config.get(SINK_FLUSH_INTERVAL),
                        config.get(SINK_MAX_RETRIES),
                        config.get(SINK_RETRY_BACKOFF),
                        config.get(SINK_ADAPTIVE_BATCH_SPLIT_ENABLED),
                        config.get(SINK_ADAPTIVE_BATCH_SPLIT_MIN_ROWS),
                        config.get(SINK_DELETE_ENABLED),
                        normalizePrimaryKeyChangeMode(config.get(SINK_PRIMARY_KEY_CHANGE_MODE)),
                        config.get(SINK_ALLOW_NO_PRIMARY_KEY),
                        config.get(CONSISTENCY_LEVEL),
                        config.get(INDEX_TYPE),
                        config.get(INDEX_METRIC_TYPE),
                        parseIndexParams(config));
        validateCollectionMappingCollisions(
                sinkConfig, tableVectorFields, parseCollisionCheckTables(config));
        return new MilvusDataSink(sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URI);
        options.add(VECTOR_FIELDS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOKEN);
        options.add(CONNECT_TIMEOUT);
        options.add(RPC_DEADLINE);
        options.add(DATABASE_NAME);
        options.add(COLLECTION_MAPPING);
        options.add(COLLECTION_COLLISION_CHECK_TABLES);
        options.add(COLLECTION_NAME_NORMALIZE_ENABLED);
        options.add(CREATE_COLLECTION_ENABLED);
        options.add(CREATE_INDEX_ENABLED);
        options.add(ENABLE_DYNAMIC_FIELD);
        options.add(LOAD_COLLECTION_ENABLED);
        options.add(PARTITION_FIELD);
        options.add(PARTITION_AUTO_CREATE_ENABLED);
        options.add(PARTITION_AUTO_CREATE_MAX_COUNT);
        options.add(PARTITION_NAMES);
        options.add(PRIMARY_KEY_FIELD);
        options.add(VARCHAR_MAX_LENGTH_DEFAULT);
        options.add(SINK_FLUSH_MAX_ROWS);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_RETRY_BACKOFF);
        options.add(SINK_ADAPTIVE_BATCH_SPLIT_ENABLED);
        options.add(SINK_ADAPTIVE_BATCH_SPLIT_MIN_ROWS);
        options.add(SINK_DELETE_ENABLED);
        options.add(SINK_PRIMARY_KEY_CHANGE_MODE);
        options.add(SINK_ALLOW_NO_PRIMARY_KEY);
        options.add(CONSISTENCY_LEVEL);
        options.add(INDEX_TYPE);
        options.add(INDEX_METRIC_TYPE);
        return options;
    }

    private List<MilvusVectorFieldSpec> parseVectorFields(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException(key + " must not be empty.");
        }
        List<MilvusVectorFieldSpec> fields = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();
        for (String definition : value.split(",")) {
            if (definition.trim().isEmpty()) {
                continue;
            }
            try {
                MilvusVectorFieldSpec field = MilvusVectorFieldSpec.parse(definition.trim());
                if (!seenFieldNames.add(field.getFieldName())) {
                    throw new ValidationException(
                            "Duplicate Milvus vector field "
                                    + field.getFieldName()
                                    + " in "
                                    + key
                                    + ".");
                }
                fields.add(field);
            } catch (IllegalArgumentException e) {
                throw new ValidationException(
                        "Invalid Milvus vector field definition for " + key, e);
            }
        }
        if (fields.isEmpty()) {
            throw new ValidationException(key + " must contain at least one vector field.");
        }
        return fields;
    }

    private Map<TableId, List<MilvusVectorFieldSpec>> parseTableVectorFields(Configuration config) {
        Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields = new HashMap<>();
        MilvusDataSinkOptions.getPropertiesByPrefix(config, VECTOR_FIELDS_TABLE_PREFIX)
                .forEach(
                        (table, definitions) -> {
                            if (table.trim().isEmpty()) {
                                throw new ValidationException(
                                        "Table-specific vector.fields key must not be empty.");
                            }
                            tableVectorFields.put(
                                    TableId.parse(table.trim()),
                                    Collections.unmodifiableList(
                                            parseVectorFields(
                                                    definitions,
                                                    VECTOR_FIELDS_TABLE_PREFIX + table)));
                        });
        return tableVectorFields;
    }

    private Map<TableId, String> parseCollectionMappings(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TableId, String> mappings = new LinkedHashMap<>();
        for (String item : value.split(";")) {
            if (item.trim().isEmpty()) {
                continue;
            }
            String[] parts = item.split(":", 2);
            if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
                throw new ValidationException(
                        COLLECTION_MAPPING.key()
                                + " is malformed. Expected tableId:collection;tableId2:collection2.");
            }
            TableId tableId = TableId.parse(parts[0].trim());
            if (mappings.containsKey(tableId)) {
                throw new ValidationException(
                        COLLECTION_MAPPING.key()
                                + " contains duplicate mapping for table "
                                + tableId
                                + ".");
            }
            mappings.put(
                    tableId, MilvusNameUtils.validateIdentifier(parts[1].trim(), "collection"));
        }
        return mappings;
    }

    private Map<String, Object> parseIndexParams(Configuration config) {
        Map<String, Object> params = new LinkedHashMap<>();
        MilvusDataSinkOptions.getPropertiesByPrefix(config, INDEX_PARAMS_PREFIX)
                .forEach(
                        (key, value) -> {
                            if (key.trim().isEmpty()) {
                                throw new ValidationException(
                                        INDEX_PARAMS_PREFIX
                                                + " keys must not be empty. Use "
                                                + INDEX_PARAMS_PREFIX
                                                + "<param-name>.");
                            }
                            params.put(key, parseScalarValue(value));
                        });
        return params;
    }

    private List<String> parsePartitionNames(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> partitionNames = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String item : value.split(",")) {
            String partitionName = item.trim();
            if (partitionName.isEmpty()) {
                continue;
            }
            if ("_default".equals(partitionName)) {
                throw new ValidationException(
                        PARTITION_NAMES.key()
                                + " must not include Milvus reserved _default partition.");
            }
            MilvusNameUtils.validateIdentifier(partitionName, "partition");
            if (!seen.add(partitionName)) {
                throw new ValidationException(
                        PARTITION_NAMES.key()
                                + " contains duplicate partition "
                                + partitionName
                                + ".");
            }
            partitionNames.add(partitionName);
        }
        return Collections.unmodifiableList(partitionNames);
    }

    private Object parseScalarValue(String value) {
        String trimmed = value.trim();
        if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
            return Boolean.parseBoolean(trimmed);
        }
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException ignored) {
            // fall through
        }
        try {
            return Double.parseDouble(trimmed);
        } catch (NumberFormatException ignored) {
            return value;
        }
    }

    private List<TableId> parseCollisionCheckTables(Configuration config) {
        String value = config.get(COLLECTION_COLLISION_CHECK_TABLES);
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<TableId> tableIds = new ArrayList<>();
        Set<TableId> seen = new HashSet<>();
        for (String item : value.split(",")) {
            String table = item.trim();
            if (table.isEmpty()) {
                continue;
            }
            TableId tableId = TableId.parse(table);
            if (!seen.add(tableId)) {
                throw new ValidationException(
                        COLLECTION_COLLISION_CHECK_TABLES.key()
                                + " contains duplicate table "
                                + tableId
                                + ".");
            }
            tableIds.add(tableId);
        }
        return tableIds;
    }

    private void validateCollectionMappingCollisions(
            MilvusDataSinkConfig config,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            List<TableId> collisionCheckTables) {
        Set<TableId> knownTables = new HashSet<>();
        knownTables.addAll(config.getCollectionMappings().keySet());
        knownTables.addAll(tableVectorFields.keySet());
        knownTables.addAll(collisionCheckTables);
        try {
            MilvusCollectionUtils.validateNormalizedCollectionCollisions(knownTables, config);
        } catch (IllegalStateException e) {
            throw new ValidationException(e.getMessage(), e);
        }
    }

    private void validatePositive(int value, String key) {
        if (value <= 0) {
            throw new ValidationException(key + " must be greater than 0.");
        }
    }

    private void validateNonNegative(int value, String key) {
        if (value < 0) {
            throw new ValidationException(key + " must be greater than or equal to 0.");
        }
    }

    private void validatePositiveDuration(Duration value, String key) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new ValidationException(key + " must be greater than 0.");
        }
    }

    private void validateNonNegativeDuration(Duration value, String key) {
        if (value == null || value.isNegative()) {
            throw new ValidationException(key + " must be greater than or equal to 0.");
        }
    }

    private <E extends Enum<E>> void validateEnum(Class<E> enumClass, String value, String key) {
        try {
            Enum.valueOf(enumClass, value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid value for " + key + ": " + value, e);
        }
    }

    private void validatePrimaryKeyChangeMode(String value) {
        String normalized = normalizePrimaryKeyChangeMode(value);
        if (!"reject".equals(normalized) && !"allow".equals(normalized)) {
            throw new ValidationException(
                    SINK_PRIMARY_KEY_CHANGE_MODE.key()
                            + " must be either reject or allow, but was "
                            + value
                            + ".");
        }
    }

    private String normalizePrimaryKeyChangeMode(String value) {
        return value == null ? "reject" : value.trim().toLowerCase(Locale.ROOT);
    }
}
