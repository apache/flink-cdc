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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Serializable runtime configuration for the Milvus sink. */
public class MilvusDataSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String token;
    private final Duration connectTimeout;
    private final Duration rpcDeadline;
    private final String databaseName;
    private final Map<TableId, String> collectionMappings;
    private final boolean collectionNameNormalizeEnabled;
    private final boolean createCollectionEnabled;
    private final boolean createIndexEnabled;
    private final boolean enableDynamicField;
    private final boolean loadCollectionEnabled;
    private final String partitionField;
    private final boolean partitionAutoCreateEnabled;
    private final int partitionAutoCreateMaxCount;
    private final List<String> partitionNames;
    private final List<MilvusVectorFieldSpec> vectorFields;
    private final Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields;
    private final String primaryKeyField;
    private final int varcharMaxLengthDefault;
    private final int flushMaxRows;
    private final Duration flushInterval;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final boolean adaptiveBatchSplitEnabled;
    private final int adaptiveBatchSplitMinRows;
    private final boolean deleteEnabled;
    private final String primaryKeyChangeMode;
    private final boolean allowNoPrimaryKey;
    private final String consistencyLevel;
    private final String indexType;
    private final String indexMetricType;
    private final Map<String, Object> indexParams;

    public MilvusDataSinkConfig(
            String uri,
            String token,
            Duration connectTimeout,
            Duration rpcDeadline,
            String databaseName,
            Map<TableId, String> collectionMappings,
            boolean collectionNameNormalizeEnabled,
            boolean createCollectionEnabled,
            boolean createIndexEnabled,
            boolean enableDynamicField,
            boolean loadCollectionEnabled,
            String partitionField,
            boolean partitionAutoCreateEnabled,
            int partitionAutoCreateMaxCount,
            List<String> partitionNames,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            String primaryKeyField,
            int varcharMaxLengthDefault,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean adaptiveBatchSplitEnabled,
            int adaptiveBatchSplitMinRows,
            boolean deleteEnabled,
            String primaryKeyChangeMode,
            boolean allowNoPrimaryKey,
            String consistencyLevel,
            String indexType,
            String indexMetricType,
            Map<String, Object> indexParams) {
        this.uri = uri;
        this.token = token;
        this.connectTimeout = connectTimeout;
        this.rpcDeadline = rpcDeadline;
        this.databaseName = databaseName;
        this.collectionMappings = Collections.unmodifiableMap(collectionMappings);
        this.collectionNameNormalizeEnabled = collectionNameNormalizeEnabled;
        this.createCollectionEnabled = createCollectionEnabled;
        this.createIndexEnabled = createIndexEnabled;
        this.enableDynamicField = enableDynamicField;
        this.loadCollectionEnabled = loadCollectionEnabled;
        this.partitionField = partitionField;
        this.partitionAutoCreateEnabled = partitionAutoCreateEnabled;
        this.partitionAutoCreateMaxCount = partitionAutoCreateMaxCount;
        this.partitionNames = Collections.unmodifiableList(partitionNames);
        this.vectorFields = Collections.unmodifiableList(vectorFields);
        this.tableVectorFields = Collections.unmodifiableMap(tableVectorFields);
        this.primaryKeyField = primaryKeyField;
        this.varcharMaxLengthDefault = varcharMaxLengthDefault;
        this.flushMaxRows = flushMaxRows;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
        this.adaptiveBatchSplitEnabled = adaptiveBatchSplitEnabled;
        this.adaptiveBatchSplitMinRows = adaptiveBatchSplitMinRows;
        this.deleteEnabled = deleteEnabled;
        this.primaryKeyChangeMode = primaryKeyChangeMode;
        this.allowNoPrimaryKey = allowNoPrimaryKey;
        this.consistencyLevel = consistencyLevel;
        this.indexType = indexType;
        this.indexMetricType = indexMetricType;
        this.indexParams = Collections.unmodifiableMap(indexParams);
    }

    public String getUri() {
        return uri;
    }

    public String getToken() {
        return token;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public Duration getRpcDeadline() {
        return rpcDeadline;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Map<TableId, String> getCollectionMappings() {
        return collectionMappings;
    }

    public boolean isCollectionNameNormalizeEnabled() {
        return collectionNameNormalizeEnabled;
    }

    public boolean isCreateCollectionEnabled() {
        return createCollectionEnabled;
    }

    public boolean isCreateIndexEnabled() {
        return createIndexEnabled;
    }

    public boolean isEnableDynamicField() {
        return enableDynamicField;
    }

    public boolean isLoadCollectionEnabled() {
        return loadCollectionEnabled;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public boolean isPartitionAutoCreateEnabled() {
        return partitionAutoCreateEnabled;
    }

    public int getPartitionAutoCreateMaxCount() {
        return partitionAutoCreateMaxCount;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public List<MilvusVectorFieldSpec> getVectorFields(TableId tableId) {
        return tableVectorFields.getOrDefault(tableId, vectorFields);
    }

    public List<MilvusVectorFieldSpec> getVectorFields() {
        return vectorFields;
    }

    public Map<TableId, List<MilvusVectorFieldSpec>> getTableVectorFields() {
        return tableVectorFields;
    }

    public String getPrimaryKeyField() {
        return primaryKeyField;
    }

    public int getVarcharMaxLengthDefault() {
        return varcharMaxLengthDefault;
    }

    public int getFlushMaxRows() {
        return flushMaxRows;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    public boolean isAdaptiveBatchSplitEnabled() {
        return adaptiveBatchSplitEnabled;
    }

    public int getAdaptiveBatchSplitMinRows() {
        return adaptiveBatchSplitMinRows;
    }

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public String getPrimaryKeyChangeMode() {
        return primaryKeyChangeMode;
    }

    public boolean isAllowNoPrimaryKey() {
        return allowNoPrimaryKey;
    }

    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getIndexMetricType() {
        return indexMetricType;
    }

    public Map<String, Object> getIndexParams() {
        return indexParams;
    }
}
