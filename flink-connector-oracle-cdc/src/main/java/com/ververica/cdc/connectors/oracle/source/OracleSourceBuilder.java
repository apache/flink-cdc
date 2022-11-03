/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.annotation.Internal;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link OracleIncrementalSource} to make it easier for the users to
 * construct a {@link OracleIncrementalSource}.
 *
 * <p>Check the Java docs of each individual method to learn more about the settings to build a
 * {@link OracleIncrementalSource}.
 */
@Internal
public class OracleSourceBuilder<T> {
    private final OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
    private RedoLogOffsetFactory offsetFactory;
    private OracleDialect dialect;
    private DebeziumDeserializationSchema<T> deserializer;

    public OracleSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Url to use when connecting to the Oracle database server. */
    public OracleSourceBuilder<T> url(@Nullable String url) {
        this.configFactory.url(url);
        return this;
    }

    /** Integer port number of the Oracle database server. */
    public OracleSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /**
     * An required list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring.
     */
    public OracleSourceBuilder<T> databaseList(String... databaseList) {
        this.configFactory.databaseList(databaseList);
        return this;
    }

    /**
     * An optional list of regular expressions that match schema names to be monitored; any schema
     * name not included in the whitelist will be excluded from monitoring. By default all
     * non-system schemas will be monitored.
     */
    public OracleSourceBuilder<T> schemaList(String... schemaList) {
        this.configFactory.schemaList(schemaList);
        return this;
    }

    /**
     * An required list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form {@code <databaseName>.<tableName>}.
     */
    public OracleSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** Name of the Oracle database to use when connecting to the Oracle database server. */
    public OracleSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the Oracle database server. */
    public OracleSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in Oracle converted to STRING. See more
     * https://debezium.io/documentation/reference/1.5/connectors/Oracle.html#Oracle-temporal-types
     */
    public OracleSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public OracleSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be will
     * be divided into multiple groups.
     */
    public OracleSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public OracleSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public OracleSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public OracleSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the Oracle
     * database server before timing out.
     */
    public OracleSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public OracleSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The connection pool size. */
    public OracleSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** Whether the {@link OracleIncrementalSource} should output the schema changes or not. */
    public OracleSourceBuilder<T> includeSchemaChanges(boolean includeSchemaChanges) {
        this.configFactory.includeSchemaChanges(includeSchemaChanges);
        return this;
    }

    /** Specifies the startup options. */
    public OracleSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /** The Debezium Oracle connector properties. For example, "snapshot.mode". */
    public OracleSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public OracleSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Build the {@link OracleIncrementalSource}.
     *
     * @return a OracleParallelSource with the settings made for this builder.
     */
    public OracleIncrementalSource<T> build() {
        this.offsetFactory = new RedoLogOffsetFactory();
        this.dialect = new OracleDialect(configFactory);
        return new OracleIncrementalSource<T>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    /** The {@link JdbcIncrementalSource} implementation for Oracle. */
    public static class OracleIncrementalSource<T> extends JdbcIncrementalSource<T> {

        public OracleIncrementalSource(
                OracleSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                RedoLogOffsetFactory offsetFactory,
                OracleDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }

        public static <T> OracleSourceBuilder<T> builder() {
            return new OracleSourceBuilder<>();
        }
    }
}
