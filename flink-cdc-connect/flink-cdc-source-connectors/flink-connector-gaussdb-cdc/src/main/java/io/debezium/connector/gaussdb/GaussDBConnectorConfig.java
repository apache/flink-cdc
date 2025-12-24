/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Duration;

/**
 * The configuration properties for the GaussDB connector.
 *
 * <p>GaussDB speaks PostgreSQL protocol, so it reuses common PostgreSQL-style JDBC settings and
 * replication-related properties like slot name and decoding plugin.
 */
public class GaussDBConnectorConfig extends RelationalDatabaseConnectorConfig {

    public static final int DEFAULT_PORT = 8_000;
    private static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;
    private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 30_000L;
    private static final int DEFAULT_CONNECT_MAX_RETRIES = 3;
    private static final long DEFAULT_CONNECT_RETRY_DELAY_MS = 5_000L;

    public static final Field PORT =
            RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);

    public static final Field CONNECTION_TIMEOUT_MS =
            Field.create(
                            CommonConnectorConfig.DATABASE_CONFIG_PREFIX
                                    + JdbcConfiguration.CONNECTION_TIMEOUT_MS.name())
                    .withDisplayName("Connection Timeout (ms)")
                    .withType(Type.LONG)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
                    .withWidth(Width.MEDIUM)
                    .withImportance(Importance.MEDIUM)
                    .withDefault(DEFAULT_CONNECTION_TIMEOUT_MS)
                    .withValidation(Field::isNonNegativeLong)
                    .withDescription(
                            "The maximum time in milliseconds that the connector should wait after trying to connect to the database server before timing out.");

    public static final Field CONNECT_MAX_RETRIES =
            Field.create(
                            CommonConnectorConfig.DATABASE_CONFIG_PREFIX
                                    + "gaussdb.connect.max.retries")
                    .withDisplayName("Connection Max Retries")
                    .withType(Type.INT)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
                    .withWidth(Width.MEDIUM)
                    .withImportance(Importance.MEDIUM)
                    .withDefault(DEFAULT_CONNECT_MAX_RETRIES)
                    .withValidation(Field::isPositiveInteger)
                    .withDescription(
                            "The maximum number of retries for establishing a GaussDB JDBC connection.");

    public static final Field CONNECT_RETRY_DELAY_MS =
            Field.create(
                            CommonConnectorConfig.DATABASE_CONFIG_PREFIX
                                    + "gaussdb.connect.retry.delay.ms")
                    .withDisplayName("Connection Retry Delay (ms)")
                    .withType(Type.LONG)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
                    .withWidth(Width.MEDIUM)
                    .withImportance(Importance.MEDIUM)
                    .withDefault(DEFAULT_CONNECT_RETRY_DELAY_MS)
                    .withValidation(Field::isNonNegativeLong)
                    .withDescription(
                            "The delay in milliseconds between retries for establishing a GaussDB JDBC connection.");

    public static final Field PLUGIN_NAME =
            Field.create("plugin.name")
                    .withDisplayName("Plugin")
                    .withType(Type.STRING)
                    .withGroup(
                            Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 0))
                    .withWidth(Width.MEDIUM)
                    .withImportance(Importance.MEDIUM)
                    .withDescription(
                            "The name of the GaussDB logical decoding plugin installed on the server.");

    public static final Field SLOT_NAME =
            Field.create("slot.name")
                    .withDisplayName("Slot")
                    .withType(Type.STRING)
                    .withGroup(
                            Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 1))
                    .withWidth(Width.MEDIUM)
                    .withImportance(Importance.MEDIUM)
                    .withDescription(
                            "The name of the GaussDB logical decoding slot that the server uses to stream changes.");

    private static final ConfigDefinition CONFIG_DEFINITION =
            RelationalDatabaseConnectorConfig.CONFIG_DEFINITION
                    .edit()
                    .name("GaussDB")
                    .type(
                            HOSTNAME,
                            PORT,
                            USER,
                            PASSWORD,
                            DATABASE_NAME,
                            CONNECTION_TIMEOUT_MS,
                            CONNECT_MAX_RETRIES,
                            CONNECT_RETRY_DELAY_MS,
                            PLUGIN_NAME,
                            SLOT_NAME)
                    .create();

    /** The set of {@link Field}s defined as part of this configuration. */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public GaussDBConnectorConfig(Configuration config) {
        super(
                config,
                config.getString(RelationalDatabaseConnectorConfig.SERVER_NAME),
                tableId -> true,
                // Use schema.table format to match PostgreSQL-style table.include.list pattern
                x -> x.schema() + "." + x.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA);
    }

    @Override
    public String getContextName() {
        return "gaussdb";
    }

    @Override
    public String getConnectorName() {
        return "gaussdb";
    }

    public String pluginName() {
        return getConfig().getString(PLUGIN_NAME);
    }

    public String slotName() {
        return getConfig().getString(SLOT_NAME);
    }

    public Duration connectionTimeout() {
        return Duration.ofMillis(getConfig().getLong(CONNECTION_TIMEOUT_MS));
    }

    public int connectMaxRetries() {
        return getConfig().getInteger(CONNECT_MAX_RETRIES);
    }

    public Duration connectRetryDelay() {
        return Duration.ofMillis(getConfig().getLong(CONNECT_RETRY_DELAY_MS));
    }

    @Override
    public boolean validate(Iterable<Field> fields, Field.ValidationOutput problems) {
        boolean result = super.validate(fields, problems);
        if (getConfig().getLong(CONNECTION_TIMEOUT_MS) == 0L) {
            problems.accept(
                    CONNECTION_TIMEOUT_MS,
                    getConfig().getLong(CONNECTION_TIMEOUT_MS),
                    "Connection timeout should be greater than 0 ms.");
            result = false;
        }
        return result;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(
            Version version) {
        return new GaussDBSourceInfoStructMaker("gaussdb", "unknown", this);
    }

    private static class GaussDBSourceInfoStructMaker
            extends AbstractSourceInfoStructMaker<AbstractSourceInfo> {

        private final Schema schema;

        GaussDBSourceInfoStructMaker(
                String connectorName, String connectorVersion, GaussDBConnectorConfig config) {
            super(connectorName, connectorVersion, config);
            this.schema =
                    commonSchemaBuilder()
                            .name("io.debezium.connector.gaussdb.Source")
                            .field(
                                    AbstractSourceInfo.SCHEMA_NAME_KEY,
                                    SchemaBuilder.string().optional().build())
                            .field(
                                    AbstractSourceInfo.TABLE_NAME_KEY,
                                    SchemaBuilder.string().optional().build())
                            .build();
        }

        @Override
        public org.apache.kafka.connect.data.Schema schema() {
            return schema;
        }

        @Override
        public org.apache.kafka.connect.data.Struct struct(AbstractSourceInfo sourceInfo) {
            return commonStruct(sourceInfo);
        }
    }
}
