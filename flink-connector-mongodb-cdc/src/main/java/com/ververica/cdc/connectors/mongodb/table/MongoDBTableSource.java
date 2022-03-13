/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.MetadataConverter;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a MongoDB change stream events source
 * from a logical description.
 */
public class MongoDBTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;
    private final String hosts;
    private final String connectionOptions;
    private final String username;
    private final String password;
    private final String database;
    private final String collection;
    private final Boolean errorsLogEnable;
    private final String errorsTolerance;
    private final Boolean copyExisting;
    private final String copyExistingPipeline;
    private final Integer copyExistingMaxThreads;
    private final Integer copyExistingQueueSize;
    private final Integer pollMaxBatchSize;
    private final Integer pollAwaitTimeMillis;
    private final Integer heartbeatIntervalMillis;
    private final ZoneId localTimeZone;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public MongoDBTableSource(
            ResolvedSchema physicalSchema,
            String hosts,
            @Nullable String username,
            @Nullable String password,
            String database,
            String collection,
            @Nullable String connectionOptions,
            @Nullable String errorsTolerance,
            @Nullable Boolean errorsLogEnable,
            @Nullable Boolean copyExisting,
            @Nullable String copyExistingPipeline,
            @Nullable Integer copyExistingMaxThreads,
            @Nullable Integer copyExistingQueueSize,
            @Nullable Integer pollMaxBatchSize,
            @Nullable Integer pollAwaitTimeMillis,
            @Nullable Integer heartbeatIntervalMillis,
            ZoneId localTimeZone) {
        this.physicalSchema = physicalSchema;
        this.hosts = checkNotNull(hosts);
        this.username = username;
        this.password = password;
        this.database = checkNotNull(database);
        this.collection = checkNotNull(collection);
        this.connectionOptions = connectionOptions;
        this.errorsTolerance = errorsTolerance;
        this.errorsLogEnable = errorsLogEnable;
        this.copyExisting = copyExisting;
        this.copyExistingPipeline = copyExistingPipeline;
        this.copyExistingMaxThreads = copyExistingMaxThreads;
        this.copyExistingQueueSize = copyExistingQueueSize;
        this.pollMaxBatchSize = pollMaxBatchSize;
        this.pollAwaitTimeMillis = pollAwaitTimeMillis;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.localTimeZone = localTimeZone;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = getMetadataConverters();
        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(producedDataType);

        DebeziumDeserializationSchema<RowData> deserializer =
                new MongoDBConnectorDeserializationSchema(
                        physicalDataType, metadataConverters, typeInfo, localTimeZone);

        MongoDBSource.Builder<RowData> builder =
                MongoDBSource.<RowData>builder()
                        .hosts(hosts)
                        .database(database)
                        .collection(collection)
                        .deserializer(deserializer);

        Optional.ofNullable(username).ifPresent(builder::username);
        Optional.ofNullable(password).ifPresent(builder::password);
        Optional.ofNullable(connectionOptions).ifPresent(builder::connectionOptions);
        Optional.ofNullable(errorsLogEnable).ifPresent(builder::errorsLogEnable);
        Optional.ofNullable(errorsTolerance).ifPresent(builder::errorsTolerance);
        Optional.ofNullable(copyExisting).ifPresent(builder::copyExisting);
        Optional.ofNullable(copyExistingPipeline).ifPresent(builder::copyExistingPipeline);
        Optional.ofNullable(copyExistingMaxThreads).ifPresent(builder::copyExistingMaxThreads);
        Optional.ofNullable(copyExistingQueueSize).ifPresent(builder::copyExistingQueueSize);
        Optional.ofNullable(pollMaxBatchSize).ifPresent(builder::pollMaxBatchSize);
        Optional.ofNullable(pollAwaitTimeMillis).ifPresent(builder::pollAwaitTimeMillis);
        Optional.ofNullable(heartbeatIntervalMillis).ifPresent(builder::heartbeatIntervalMillis);

        DebeziumSourceFunction<RowData> sourceFunction = builder.build();

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    protected MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(MongoDBReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(MongoDBReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(MongoDBReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                MongoDBReadableMetadata::getKey,
                                MongoDBReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        MongoDBTableSource source =
                new MongoDBTableSource(
                        physicalSchema,
                        hosts,
                        username,
                        password,
                        database,
                        collection,
                        connectionOptions,
                        errorsTolerance,
                        errorsLogEnable,
                        copyExisting,
                        copyExistingPipeline,
                        copyExistingMaxThreads,
                        copyExistingQueueSize,
                        pollMaxBatchSize,
                        pollAwaitTimeMillis,
                        heartbeatIntervalMillis,
                        localTimeZone);
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDBTableSource that = (MongoDBTableSource) o;
        return Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hosts, that.hosts)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection)
                && Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(errorsTolerance, that.errorsTolerance)
                && Objects.equals(errorsLogEnable, that.errorsLogEnable)
                && Objects.equals(copyExisting, that.copyExisting)
                && Objects.equals(copyExistingPipeline, that.copyExistingPipeline)
                && Objects.equals(copyExistingMaxThreads, that.copyExistingMaxThreads)
                && Objects.equals(copyExistingQueueSize, that.copyExistingQueueSize)
                && Objects.equals(pollMaxBatchSize, that.pollMaxBatchSize)
                && Objects.equals(pollAwaitTimeMillis, that.pollAwaitTimeMillis)
                && Objects.equals(heartbeatIntervalMillis, that.heartbeatIntervalMillis)
                && Objects.equals(localTimeZone, that.localTimeZone)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                hosts,
                username,
                password,
                database,
                collection,
                connectionOptions,
                errorsTolerance,
                errorsLogEnable,
                copyExisting,
                copyExistingPipeline,
                copyExistingMaxThreads,
                copyExistingQueueSize,
                pollMaxBatchSize,
                pollAwaitTimeMillis,
                heartbeatIntervalMillis,
                localTimeZone,
                producedDataType,
                metadataKeys);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB-CDC";
    }
}
