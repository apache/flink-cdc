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

package com.ververica.cdc.connectors.postgres.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.ververica.cdc.debezium.table.DebeziumChangelogMode;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Factory for creating configured instance of {@link PostgreSQLTableSource}. */
public class PostgreSQLTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "postgres-cdc";

    private static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the PostgreSQL database server.");

    private static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Integer port number of the PostgreSQL database server.");

    private static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the PostgreSQL database to use when connecting to the PostgreSQL database server.");

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the PostgreSQL database server.");

    private static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the PostgreSQL server to monitor.");

    private static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Schema name of the PostgreSQL database to monitor.");

    private static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the PostgreSQL database to monitor.");

    private static final ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("decoderbufs")
                    .withDescription(
                            "The name of the Postgres logical decoding plug-in installed on the server.\n"
                                    + "Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,\n"
                                    + "wal2json_rds_streaming and pgoutput.");

    private static final ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the PostgreSQL logical decoding slot that was created for streaming changes "
                                    + "from a particular plug-in for a particular database/schema. The server uses this slot "
                                    + "to stream events to the connector that you are configuring.");

    private static final ConfigOption<DebeziumChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .enumType(DebeziumChangelogMode.class)
                    .defaultValue(DebeziumChangelogMode.ALL)
                    .withDescription(
                            "The changelog mode used for encoding streaming changes.\n"
                                    + "\"all\": Encodes changes as retract stream using all RowKinds. This is the default mode.\n"
                                    + "\"upsert\": Encodes changes as upsert stream that describes idempotent updates on a key. It can be used for tables with primary keys when replica identity FULL is not an option.");

    @Override
    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tableName = config.get(TABLE_NAME);
        int port = config.get(PORT);
        String pluginName = config.get(DECODING_PLUGIN_NAME);
        String slotName = config.get(SLOT_NAME);
        DebeziumChangelogMode changelogMode = config.get(CHANGELOG_MODE);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        if (changelogMode == DebeziumChangelogMode.UPSERT) {
            checkArgument(
                    physicalSchema.getPrimaryKey().isPresent(),
                    "Primary key must be present when upsert mode is selected.");
        }

        return new PostgreSQLTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                schemaName,
                tableName,
                username,
                password,
                pluginName,
                slotName,
                changelogMode,
                getDebeziumProperties(context.getCatalogTable().getOptions()));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        options.add(SLOT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(DECODING_PLUGIN_NAME);
        options.add(CHANGELOG_MODE);
        return options;
    }
}
