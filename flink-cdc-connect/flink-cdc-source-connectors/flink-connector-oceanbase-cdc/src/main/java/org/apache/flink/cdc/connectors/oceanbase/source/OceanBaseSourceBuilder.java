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

package org.apache.flink.cdc.connectors.oceanbase.source;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseSourceConfigFactory;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.LogMessageOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** The builder class for {@link OceanBaseIncrementalSource}. */
public class OceanBaseSourceBuilder<T> {

    private final OceanBaseSourceConfigFactory configFactory = new OceanBaseSourceConfigFactory();
    private LogMessageOffsetFactory offsetFactory;
    private OceanBaseDialect dialect;
    private DebeziumDeserializationSchema<T> deserializer;

    /** Specifies the startup options. */
    public OceanBaseSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /** Hostname of the OceanBase database. */
    public OceanBaseSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the OceanBase database. */
    public OceanBaseSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /** The compatible mode of the OceanBase database. */
    public OceanBaseSourceBuilder<T> compatibleMode(String compatibleMode) {
        this.configFactory.compatibleMode(compatibleMode);
        return this;
    }

    /** The jdbc driver class used to connect to the OceanBase database. */
    public OceanBaseSourceBuilder<T> driverClassName(String driverClassName) {
        this.configFactory.driverClassName(driverClassName);
        return this;
    }

    //    /** Custom properties that will overwrite the default JDBC connection URL. */
    //    public OceanBaseSourceBuilder<T> jdbcProperties(Properties properties) {
    //        this.configFactory.jdbcProperties(properties);
    //        return this;
    //    }

    /** The tenant name of OceanBase database to be monitored. */
    public OceanBaseSourceBuilder<T> tenantName(String tenantName) {
        this.configFactory.tenantName(tenantName);
        return this;
    }

    /**
     * A list of regular expressions that match database names to be monitored; any database name
     * not included in the whitelist will be excluded from monitoring.
     */
    public OceanBaseSourceBuilder<T> databaseList(String... databaseList) {
        this.configFactory.databaseList(databaseList);
        return this;
    }

    /**
     * A list of regular expressions that match fully-qualified table identifiers for tables to be
     * monitored; any table not included in the list will be excluded from monitoring. Each
     * identifier is of the form {@code <databaseName>.<tableName>}.
     */
    public OceanBaseSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /**
     * Username of the OceanBase database to use when connecting to the OceanBase database server.
     */
    public OceanBaseSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the OceanBase database server. */
    public OceanBaseSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /** Hostname of OceanBase Log Proxy server. */
    public OceanBaseSourceBuilder<T> logProxyHost(String logProxyHost) {
        this.configFactory.logProxyHost(logProxyHost);
        return this;
    }

    /** Integer port number of OceanBase Log Proxy server. */
    public OceanBaseSourceBuilder<T> logProxyPort(Integer logProxyPort) {
        this.configFactory.logProxyPort(logProxyPort);
        return this;
    }

    /** The root server list of OceanBase database, required by OceanBase Community Edition. */
    public OceanBaseSourceBuilder<T> rsList(String rsList) {
        this.configFactory.rsList(rsList);
        return this;
    }

    /** The config url of OceanBase database, required by OceanBase Enterprise Edition. */
    public OceanBaseSourceBuilder<T> configUrl(String configUrl) {
        this.configFactory.configUrl(configUrl);
        return this;
    }

    /** The working mode of libobcdc, can be 'memory' or 'storage'. */
    public OceanBaseSourceBuilder<T> workingMode(String workingMode) {
        this.configFactory.workingMode(workingMode);
        return this;
    }

    /** Custom libobcdc properties. */
    public OceanBaseSourceBuilder<T> obcdcProperties(Properties properties) {
        this.configFactory.obcdcProperties(properties);
        return this;
    }

    /** The Debezium properties. */
    public OceanBaseSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in OceanBase converted to STRING.
     */
    public OceanBaseSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the OceanBase
     * database server before timing out.
     */
    public OceanBaseSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The connection pool size. */
    public OceanBaseSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** The max retry times to get connection. */
    public OceanBaseSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public OceanBaseSourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public OceanBaseSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public OceanBaseSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public OceanBaseSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public OceanBaseSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public OceanBaseSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /**
     * Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public OceanBaseSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /** Whether the {@link OceanBaseIncrementalSource} should scan the newly added table. */
    public OceanBaseSourceBuilder<T> scanNewlyAddedTableEnabled(
            boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public OceanBaseSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public OceanBaseIncrementalSource<T> build() {
        this.offsetFactory = new LogMessageOffsetFactory();
        this.dialect = new OceanBaseDialect();
        return new OceanBaseIncrementalSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    /** The {@link JdbcIncrementalSource} implementation for OceanBase. */
    public static class OceanBaseIncrementalSource<T> extends JdbcIncrementalSource<T> {
        public OceanBaseIncrementalSource(
                OceanBaseSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                LogMessageOffsetFactory offsetFactory,
                OceanBaseDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }

        public static <T> OceanBaseSourceBuilder<T> builder() {
            return new OceanBaseSourceBuilder<>();
        }
    }
}
