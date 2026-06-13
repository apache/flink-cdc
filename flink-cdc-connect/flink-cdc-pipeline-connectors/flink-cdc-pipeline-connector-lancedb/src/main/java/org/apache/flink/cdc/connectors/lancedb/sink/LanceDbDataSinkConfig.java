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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.event.TableId;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

/** Serializable configuration for LanceDB pipeline sink. */
public class LanceDbDataSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String rootPath;
    private final Map<TableId, String> tablePathMapping;
    private final boolean tableNameNormalizeEnabled;
    private final boolean createTableEnabled;
    private final boolean schemaValidationEnabled;
    private final boolean schemaEvolutionEnabled;
    private final String changelogMode;
    private final int flushMaxRows;
    private final int maxRowsPerCommit;
    private final Duration flushInterval;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final int maxRowsPerFile;
    private final int maxRowsPerGroup;
    private final long maxBytesPerFile;
    private final boolean enableStableRowIds;
    private final String writeMode;
    private final ZoneId zoneId;
    private final Map<String, String> storageOptions;

    public LanceDbDataSinkConfig(
            String rootPath,
            Map<TableId, String> tablePathMapping,
            boolean tableNameNormalizeEnabled,
            boolean createTableEnabled,
            boolean schemaValidationEnabled,
            boolean schemaEvolutionEnabled,
            String changelogMode,
            int flushMaxRows,
            int maxRowsPerCommit,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            int maxRowsPerFile,
            int maxRowsPerGroup,
            long maxBytesPerFile,
            boolean enableStableRowIds,
            String writeMode,
            ZoneId zoneId,
            Map<String, String> storageOptions) {
        this.rootPath = rootPath;
        this.tablePathMapping = Collections.unmodifiableMap(tablePathMapping);
        this.tableNameNormalizeEnabled = tableNameNormalizeEnabled;
        this.createTableEnabled = createTableEnabled;
        this.schemaValidationEnabled = schemaValidationEnabled;
        this.schemaEvolutionEnabled = schemaEvolutionEnabled;
        this.changelogMode = changelogMode;
        this.flushMaxRows = flushMaxRows;
        this.maxRowsPerCommit = maxRowsPerCommit;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
        this.maxRowsPerFile = maxRowsPerFile;
        this.maxRowsPerGroup = maxRowsPerGroup;
        this.maxBytesPerFile = maxBytesPerFile;
        this.enableStableRowIds = enableStableRowIds;
        this.writeMode = writeMode;
        this.zoneId = zoneId;
        this.storageOptions = Collections.unmodifiableMap(storageOptions);
    }

    public String getRootPath() {
        return rootPath;
    }

    public Map<TableId, String> getTablePathMapping() {
        return tablePathMapping;
    }

    public boolean isTableNameNormalizeEnabled() {
        return tableNameNormalizeEnabled;
    }

    public boolean isCreateTableEnabled() {
        return createTableEnabled;
    }

    public boolean isSchemaValidationEnabled() {
        return schemaValidationEnabled;
    }

    public boolean isSchemaEvolutionEnabled() {
        return schemaEvolutionEnabled;
    }

    public String getChangelogMode() {
        return changelogMode;
    }

    public int getFlushMaxRows() {
        return flushMaxRows;
    }

    public int getMaxRowsPerCommit() {
        return maxRowsPerCommit;
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

    public int getMaxRowsPerFile() {
        return maxRowsPerFile;
    }

    public int getMaxRowsPerGroup() {
        return maxRowsPerGroup;
    }

    public long getMaxBytesPerFile() {
        return maxBytesPerFile;
    }

    public boolean isEnableStableRowIds() {
        return enableStableRowIds;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public Map<String, String> getStorageOptions() {
        return storageOptions;
    }
}
