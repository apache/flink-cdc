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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.event.TableId;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Serializable runtime configuration for the TDengine sink. */
public class TDengineDataSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String databaseName;
    private final String stableName;
    private final Map<TableId, String> stableMappings;
    private final boolean nameNormalizeEnabled;
    private final String timestampField;
    private final String subtableField;
    private final List<String> tagFields;
    private final boolean createDatabaseEnabled;
    private final boolean createStableEnabled;
    private final boolean stableSchemaValidationEnabled;
    private final int varcharMaxLengthDefault;
    private final String stringType;
    private final String decimalMapping;
    private final ZoneId zoneId;
    private final int flushMaxRows;
    private final int maxSqlBytes;
    private final Duration flushInterval;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final String deleteMode;
    private final String updateMode;
    private final String timestampChangeMode;
    private final String subtableChangeMode;
    private final Map<String, String> connectionProperties;

    public TDengineDataSinkConfig(
            String url,
            String username,
            String password,
            String databaseName,
            String stableName,
            Map<TableId, String> stableMappings,
            boolean nameNormalizeEnabled,
            String timestampField,
            String subtableField,
            List<String> tagFields,
            boolean createDatabaseEnabled,
            boolean createStableEnabled,
            boolean stableSchemaValidationEnabled,
            int varcharMaxLengthDefault,
            String stringType,
            String decimalMapping,
            ZoneId zoneId,
            int flushMaxRows,
            int maxSqlBytes,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            String deleteMode,
            String updateMode,
            String timestampChangeMode,
            String subtableChangeMode,
            Map<String, String> connectionProperties) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.stableName = stableName;
        this.stableMappings = Collections.unmodifiableMap(stableMappings);
        this.nameNormalizeEnabled = nameNormalizeEnabled;
        this.timestampField = timestampField;
        this.subtableField = subtableField;
        this.tagFields = Collections.unmodifiableList(tagFields);
        this.createDatabaseEnabled = createDatabaseEnabled;
        this.createStableEnabled = createStableEnabled;
        this.stableSchemaValidationEnabled = stableSchemaValidationEnabled;
        this.varcharMaxLengthDefault = varcharMaxLengthDefault;
        this.stringType = stringType;
        this.decimalMapping = decimalMapping;
        this.zoneId = zoneId;
        this.flushMaxRows = flushMaxRows;
        this.maxSqlBytes = maxSqlBytes;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
        this.deleteMode = deleteMode;
        this.updateMode = updateMode;
        this.timestampChangeMode = timestampChangeMode;
        this.subtableChangeMode = subtableChangeMode;
        this.connectionProperties = Collections.unmodifiableMap(connectionProperties);
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getStableName() {
        return stableName;
    }

    public Map<TableId, String> getStableMappings() {
        return stableMappings;
    }

    public boolean isNameNormalizeEnabled() {
        return nameNormalizeEnabled;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public String getSubtableField() {
        return subtableField;
    }

    public List<String> getTagFields() {
        return tagFields;
    }

    public boolean isCreateDatabaseEnabled() {
        return createDatabaseEnabled;
    }

    public boolean isCreateStableEnabled() {
        return createStableEnabled;
    }

    public boolean isStableSchemaValidationEnabled() {
        return stableSchemaValidationEnabled;
    }

    public int getVarcharMaxLengthDefault() {
        return varcharMaxLengthDefault;
    }

    public String getStringType() {
        return stringType;
    }

    public String getDecimalMapping() {
        return decimalMapping;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public int getFlushMaxRows() {
        return flushMaxRows;
    }

    public int getMaxSqlBytes() {
        return maxSqlBytes;
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

    public String getDeleteMode() {
        return deleteMode;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public String getTimestampChangeMode() {
        return timestampChangeMode;
    }

    public String getSubtableChangeMode() {
        return subtableChangeMode;
    }

    public Map<String, String> getConnectionProperties() {
        return connectionProperties;
    }
}
