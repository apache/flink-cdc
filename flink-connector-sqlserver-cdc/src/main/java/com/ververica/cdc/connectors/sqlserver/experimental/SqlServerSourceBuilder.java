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

package com.ververica.cdc.connectors.sqlserver.experimental;

import org.apache.flink.annotation.Experimental;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.JdbcIncrementalSource;
import com.ververica.cdc.connectors.sqlserver.experimental.config.SqlServerSourceConfigFactory;
import com.ververica.cdc.connectors.sqlserver.experimental.offset.TransactionLogOffsetFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link SqlServerIncrementalSource} to make it easier for the users to
 * construct a {@link SqlServerIncrementalSource}.
 *
 * <pre>{@code
 * SqlServerIncrementalSource
 *     .<String>builder()
 *     .hostname("localhost")
 *     .port(3306)
 *     .databaseList("mydb")
 *     .tableList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .serverId(5400)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>Check the Java docs of each individual method to learn more about the settings to build a
 * {@link SqlServerIncrementalSource}.
 */
@Experimental
public class SqlServerSourceBuilder<T> {
    private final SqlServerSourceConfigFactory configFactory = new SqlServerSourceConfigFactory();
    private TransactionLogOffsetFactory offsetFactory;
    private SqlServerDialect dialect;
    private DebeziumDeserializationSchema<T> deserializer;

    /** IP address or hostname of the SQL Server database server. */
    public SqlServerSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the SQL Server database server. */
    public SqlServerSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /** The name of the SQL Server database from which to stream the changes. */
    public SqlServerSourceBuilder<T> database(String database) {
        this.configFactory.databaseList(database);
        return this;
    }

    /**
     * An optional comma-separated list of regular expressions that match fully-qualified table
     * identifiers for tables that you want Debezium to capture; any table that is not included in
     * table.include.list is excluded from capture. Each identifier is of the form
     * schemaName.tableName. By default, the connector captures all non-system tables for the
     * designated schemas. Must not be used with table.exclude.list.
     */
    public SqlServerSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** Username to use when connecting to the SQL Server database server. */
    public SqlServerSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the SQL Server database server. */
    public SqlServerSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types
     */
    public SqlServerSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public SqlServerSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public SqlServerSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public SqlServerSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public SqlServerSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public SqlServerSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the SqlServer
     * database server before timing out.
     */
    public SqlServerSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public SqlServerSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The connection pool size. */
    public SqlServerSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** Whether the {@link SqlServerIncrementalSource} should output the schema changes or not. */
    public SqlServerSourceBuilder<T> includeSchemaChanges(boolean includeSchemaChanges) {
        this.configFactory.includeSchemaChanges(includeSchemaChanges);
        return this;
    }

    /** Specifies the startup options. */
    public SqlServerSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /** The Debezium SqlServer connector properties. For example, "snapshot.mode". */
    public SqlServerSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public SqlServerSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Build the {@link SqlServerIncrementalSource}.
     *
     * @return a SqlServerParallelSource with the settings made for this builder.
     */
    public SqlServerIncrementalSource<T> build() {
        this.offsetFactory = new TransactionLogOffsetFactory();
        this.dialect = new SqlServerDialect(configFactory);
        return new SqlServerIncrementalSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    /** The {@link JdbcIncrementalSource} implementation for SqlServer. */
    public static class SqlServerIncrementalSource<T> extends JdbcIncrementalSource<T> {

        public SqlServerIncrementalSource(
                SqlServerSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                TransactionLogOffsetFactory offsetFactory,
                SqlServerDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }
    }
}
