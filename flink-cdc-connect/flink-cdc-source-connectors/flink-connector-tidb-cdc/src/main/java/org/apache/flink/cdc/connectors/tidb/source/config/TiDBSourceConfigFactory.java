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

package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import org.tikv.common.TiConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;
import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;

/** A factory to initialize {@link TiDBSourceConfig}. */
@SuppressWarnings("UnusedReturnValue")
public class TiDBSourceConfigFactory extends JdbcSourceConfigFactory {
    private static final long serialVersionUID = 1L;
    private String compatibleMode;
    private String driverClassName = "com.mysql.cj.jdbc.Driver";
    private String pdAddresses;

    private String hostMapping;
    private TiConfiguration tiConfiguration;
    private Properties tikvProperties;
    private Properties jdbcProperties;
    private Map<ObjectPath, String> chunkKeyColumns = new HashMap<>();

    public TiDBSourceConfigFactory compatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
        return this;
    }

    public TiDBSourceConfigFactory chunkKeyColumn(ObjectPath objectPath, String chunkKeyColumn) {
        this.chunkKeyColumns.put(objectPath, chunkKeyColumn);
        return this;
    }

    public TiDBSourceConfigFactory chunkKeyColumns(Map<ObjectPath, String> chunkKeyColumns) {
        this.chunkKeyColumns.putAll(chunkKeyColumns);
        return this;
    }

    public TiDBSourceConfigFactory driverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public TiDBSourceConfigFactory pdAddresses(String pdAddresses) {
        this.pdAddresses = pdAddresses;
        return this;
    }

    public TiDBSourceConfigFactory hostMapping(String hostMapping) {
        this.hostMapping = hostMapping;
        return this;
    }

    public TiDBSourceConfigFactory tikvProperties(Properties tikvProperties) {
        this.tikvProperties = tikvProperties;
        return this;
    }

    public TiDBSourceConfigFactory jdbcProperties(Properties jdbcProperties) {
        this.jdbcProperties = jdbcProperties;
        return this;
    }

    public TiDBSourceConfigFactory tiConfiguration(TiConfiguration tiConfiguration) {
        this.tiConfiguration = tiConfiguration;
        return this;
    }

    @Override
    public TiDBSourceConfig create(int subtask) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("database.server.name", "tidb_cdc");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("database.connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));

        // table filter
        // props.put("database.include.list", String.join(",", databaseList));
        if (tableList != null) {
            props.put("table.include.list", String.join(",", tableList));
        }
        // value converter
        props.put("decimal.handling.mode", "precise");
        props.put("time.precision.mode", "adaptive_time_microseconds");
        props.put("binary.handling.mode", "bytes");

        if (jdbcProperties != null) {
            props.putAll(jdbcProperties);
        }

        if (tikvProperties != null) {
            props.putAll(tikvProperties);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        return new TiDBSourceConfig(
                compatibleMode,
                startupOptions,
                databaseList,
                tableList,
                pdAddresses,
                hostMapping,
                splitSize,
                splitMetaGroupSize,
                tiConfiguration,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                chunkKeyColumns,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }
}
