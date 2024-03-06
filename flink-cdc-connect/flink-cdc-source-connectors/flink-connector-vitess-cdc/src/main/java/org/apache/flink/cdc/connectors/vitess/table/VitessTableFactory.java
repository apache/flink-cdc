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

package org.apache.flink.cdc.connectors.vitess.table;

import org.apache.flink.cdc.connectors.vitess.config.SchemaAdjustmentMode;
import org.apache.flink.cdc.connectors.vitess.config.TabletType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

/** Factory for creating configured instance of {@link VitessTableSource}. */
public class VitessTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "vitess-cdc";

    private static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname of the VTGate’s VStream server.");

    private static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(15991)
                    .withDescription("Integer port number of the VTGate’s VStream server.");

    private static final ConfigOption<String> KEYSPACE =
            ConfigOptions.key("keyspace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the keyspace (a.k.a database). If no shard is specified, it reads change events from all shards in the keyspace.");

    private static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username of the Vitess database server (VTGate gRPC).");

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password of the Vitess database server (VTGate gRPC).");

    private static final ConfigOption<String> SHARD =
            ConfigOptions.key("vitess.shard")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional name of the shard from which to stream the changes.");

    private static final ConfigOption<String> GTID =
            ConfigOptions.key("vitess.gtid")
                    .stringType()
                    .defaultValue("current")
                    .withDescription("An optional GTID position for a shard to stream from.");

    private static final ConfigOption<Boolean> STOP_ON_RESHARD =
            ConfigOptions.key("vitess.stop_on_reshard")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Controls Vitess flag stop_on_reshard.");

    private static final ConfigOption<Boolean> TOMBSTONES_ON_DELETE =
            ConfigOptions.key("tombstones.on.delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Controls whether a delete event is followed by a tombstone event.");

    private static final ConfigOption<String> SCHEMA_NAME_ADJUSTMENT_MODE =
            ConfigOptions.key("schema.name.adjustment.mode")
                    .stringType()
                    .defaultValue("avro")
                    .withDescription(
                            "Specifies how schema names should be adjusted for compatibility with the message converter used by the connector.");

    private static final ConfigOption<String> TABLET_TYPE =
            ConfigOptions.key("tablet-type")
                    .stringType()
                    .defaultValue(TabletType.RDONLY.name())
                    .withDescription(
                            "The type of Tablet (hence MySQL) from which to stream the changes:");

    private static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the MYSQL database to monitor.");

    private static final ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("decoderbufs")
                    .withDescription(
                            "The name of the Vitess logical decoding plug-in installed on the server.");

    private static final ConfigOption<String> NAME =
            ConfigOptions.key("name")
                    .stringType()
                    .defaultValue("flink")
                    .withDescription(
                            "Unique name for the connector."
                                    + " Attempting to register again with the same name will fail. "
                                    + "This property is required by all Kafka Connect connectors. Default is flink.");

    @Override
    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        int port = config.get(PORT);
        String keyspace = config.get(KEYSPACE);
        String tableName = config.get(TABLE_NAME);
        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);
        String shard = config.getOptional(SHARD).orElse(null);
        String gtid = config.get(GTID);
        Boolean stopOnReshard = config.get(STOP_ON_RESHARD);
        Boolean tombstonesOnDelete = config.get(TOMBSTONES_ON_DELETE);
        SchemaAdjustmentMode schemaNameAdjustmentMode =
                SchemaAdjustmentMode.valueOf(config.get(SCHEMA_NAME_ADJUSTMENT_MODE).toUpperCase());
        TabletType tabletType = TabletType.valueOf(config.get(TABLET_TYPE));
        String pluginName = config.get(DECODING_PLUGIN_NAME);
        String name = config.get(NAME);
        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        return new VitessTableSource(
                physicalSchema,
                port,
                hostname,
                keyspace,
                tableName,
                username,
                password,
                shard,
                gtid,
                stopOnReshard,
                tombstonesOnDelete,
                schemaNameAdjustmentMode,
                tabletType,
                pluginName,
                name,
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
        options.add(KEYSPACE);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SHARD);
        options.add(GTID);
        options.add(STOP_ON_RESHARD);
        options.add(TOMBSTONES_ON_DELETE);
        options.add(SCHEMA_NAME_ADJUSTMENT_MODE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLET_TYPE);
        options.add(DECODING_PLUGIN_NAME);
        options.add(NAME);
        return options;
    }
}
