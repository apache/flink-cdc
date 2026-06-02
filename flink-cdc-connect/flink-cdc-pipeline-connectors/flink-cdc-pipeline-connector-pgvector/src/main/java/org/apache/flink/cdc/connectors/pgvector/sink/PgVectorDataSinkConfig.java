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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorColumnSpec;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Serializable runtime configuration for the pgvector sink. */
public class PgVectorDataSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String defaultSchema;
    private final boolean createSchemaEnabled;
    private final boolean createTableEnabled;
    private final boolean createExtensionEnabled;
    private final int flushMaxRows;
    private final Duration flushInterval;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final boolean deleteEnabled;
    private final boolean allowNoPrimaryKey;
    private final Map<String, PgVectorColumnSpec> vectorColumns;
    private final Map<String, String> tableCreateProperties;

    public PgVectorDataSinkConfig(
            String jdbcUrl,
            String username,
            String password,
            String defaultSchema,
            boolean createSchemaEnabled,
            boolean createTableEnabled,
            boolean createExtensionEnabled,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled,
            boolean allowNoPrimaryKey,
            Map<String, PgVectorColumnSpec> vectorColumns,
            Map<String, String> tableCreateProperties) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.defaultSchema = defaultSchema;
        this.createSchemaEnabled = createSchemaEnabled;
        this.createTableEnabled = createTableEnabled;
        this.createExtensionEnabled = createExtensionEnabled;
        this.flushMaxRows = flushMaxRows;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
        this.deleteEnabled = deleteEnabled;
        this.allowNoPrimaryKey = allowNoPrimaryKey;
        this.vectorColumns = Collections.unmodifiableMap(new HashMap<>(vectorColumns));
        this.tableCreateProperties =
                Collections.unmodifiableMap(new HashMap<>(tableCreateProperties));
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public boolean isCreateSchemaEnabled() {
        return createSchemaEnabled;
    }

    public boolean isCreateTableEnabled() {
        return createTableEnabled;
    }

    public boolean isCreateExtensionEnabled() {
        return createExtensionEnabled;
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

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public boolean isAllowNoPrimaryKey() {
        return allowNoPrimaryKey;
    }

    public Map<String, PgVectorColumnSpec> getVectorColumns() {
        return vectorColumns;
    }

    public Map<String, String> getTableCreateProperties() {
        return tableCreateProperties;
    }
}
