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

import org.apache.flink.cdc.connectors.tidb.source.offset.TiDBSourceInfoStructMaker;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** TiDB connector configuration. */
public class TiDBConnectorConfig extends RelationalDatabaseConnectorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBConnectorConfig.class);

    protected static final String LOGICAL_NAME = "tidb_cdc_connector";
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;
    private final boolean readOnlyConnection = true;
    protected static final List<String> BUILT_IN_DB_NAMES =
            Collections.unmodifiableList(
                    Arrays.asList("information_schema", "mysql", "tidb", "LBACSYS", "ORAAUDITOR"));
    private final TiDBSourceConfig sourceConfig;

    public static final Field READ_ONLY_CONNECTION =
            Field.create("read.only")
                    .withDisplayName("Read only connection")
                    .withType(ConfigDef.Type.BOOLEAN)
                    .withDefault(false)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDescription(
                            "Switched connector to use alternative methods to deliver signals to Debezium instead of writing to signaling table");

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE =
            Field.create("bigint.unsigned.handling.mode")
                    .withDisplayName("BIGINT UNSIGNED Handling")
                    .withEnum(BigIntUnsignedHandlingMode.class, BigIntUnsignedHandlingMode.LONG)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 27))
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "Specify how BIGINT UNSIGNED columns should be represented in change events, including:"
                                    + "'precise' uses java.math.BigDecimal to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; "
                                    + "'long' (the default) represents values using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    public static final Field ENABLE_TIME_ADJUSTER =
            Field.create("enable.time.adjuster")
                    .withDisplayName("Enable Time Adjuster")
                    .withType(ConfigDef.Type.BOOLEAN)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 22))
                    .withDefault(true)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDescription(
                            "MySQL allows user to insert year value as either 2-digit or 4-digit. In case of two digit the value is automatically mapped into 1970 - 2069."
                                    + "false - delegates the implicit conversion to the database"
                                    + "true - (the default) Debezium makes the conversion");

    /** The set of predefined options for the handling mode configuration property. */
    public enum BigIntUnsignedHandlingMode implements EnumeratedValue {
        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@link BigDecimal} values, which are
         * represented in change events in a binary form. This is precise but difficult to use.
         */
        PRECISE("precise"),

        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@code long} values. This may be less
         * precise but is far easier to use.
         */
        LONG("long");

        private final String value;

        private BigIntUnsignedHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public JdbcValueConverters.BigIntUnsignedMode asBigIntUnsignedMode() {
            switch (this) {
                case LONG:
                    return JdbcValueConverters.BigIntUnsignedMode.LONG;
                case PRECISE:
                default:
                    return JdbcValueConverters.BigIntUnsignedMode.PRECISE;
            }
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static BigIntUnsignedHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (BigIntUnsignedHandlingMode option : BigIntUnsignedHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is
         *     invalid
         */
        public static BigIntUnsignedHandlingMode parse(String value, String defaultValue) {
            BigIntUnsignedHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    @Override
    public String getContextName() {
        return "TiDB";
    }

    @Override
    public String getConnectorName() {
        return "TiDB";
    }

    public String databaseName() {
        return getConfig().getString(DATABASE_NAME);
    }

    public TiDBConnectorConfig(TiDBSourceConfig sourceConfig) {
        super(
                Configuration.from(sourceConfig.getDbzProperties()),
                LOGICAL_NAME,
                Tables.TableFilter.fromPredicate(
                        tableId ->
                                "mysql".equalsIgnoreCase(sourceConfig.getCompatibleMode())
                                        ? !BUILT_IN_DB_NAMES.contains(tableId.catalog())
                                        : !BUILT_IN_DB_NAMES.contains(tableId.schema())),
                TableId::identifier,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                "mysql".equalsIgnoreCase(sourceConfig.getCompatibleMode())
                        ? ColumnFilterMode.CATALOG
                        : ColumnFilterMode.SCHEMA);
        this.sourceConfig = sourceConfig;
    }

    public TiDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new TiDBSourceInfoStructMaker();
    }

    public static final Field SERVER_NAME =
            RelationalDatabaseConnectorConfig.SERVER_NAME.withValidation(
                    CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public boolean isReadOnlyConnection() {
        return readOnlyConnection;
    }

    /** Whether to use SSL/TLS to connect to the database. */
    public enum SecureConnectionMode implements EnumeratedValue {
        /** Establish an unencrypted connection. */
        DISABLED("disabled"),

        /**
         * Establish a secure (encrypted) connection if the server supports secure connections. Fall
         * back to an unencrypted connection otherwise.
         */
        PREFERRED("preferred"),
        /**
         * Establish a secure connection if the server supports secure connections. The connection
         * attempt fails if a secure connection cannot be established.
         */
        REQUIRED("required"),
        /**
         * Like REQUIRED, but additionally verify the server TLS certificate against the configured
         * Certificate Authority (CA) certificates. The connection attempt fails if no valid
         * matching CA certificates are found.
         */
        VERIFY_CA("verify_ca"),
        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to
         * which the connection is attempted.
         */
        VERIFY_IDENTITY("verify_identity");

        private final String value;

        private SecureConnectionMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SecureConnectionMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SecureConnectionMode option : SecureConnectionMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is
         *     invalid
         */
        public static SecureConnectionMode parse(String value, String defaultValue) {
            SecureConnectionMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field SSL_MODE =
            Field.create("database.ssl.mode")
                    .withDisplayName("SSL mode")
                    .withEnum(
                            MySqlConnectorConfig.SecureConnectionMode.class,
                            MySqlConnectorConfig.SecureConnectionMode.DISABLED)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
                    .withWidth(ConfigDef.Width.MEDIUM)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "Whether to use an encrypted connection to MySQL. Options include"
                                    + "'disabled' (the default) to use an unencrypted connection; "
                                    + "'preferred' to establish a secure (encrypted) connection if the server supports secure connections, "
                                    + "but fall back to an unencrypted connection otherwise; "
                                    + "'required' to use a secure (encrypted) connection, and fail if one cannot be established; "
                                    + "'verify_ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                                    + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
                                    + "'verify_identity' like 'verify_ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_KEYSTORE =
            Field.create("database.ssl.keystore")
                    .withDisplayName("SSL Keystore")
                    .withType(ConfigDef.Type.STRING)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
                    .withWidth(ConfigDef.Width.LONG)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "The location of the key store file. "
                                    + "This is optional and can be used for two-way authentication between the client and the MySQL Server.");

    public static final Field SSL_KEYSTORE_PASSWORD =
            Field.create("database.ssl.keystore.password")
                    .withDisplayName("SSL Keystore Password")
                    .withType(ConfigDef.Type.PASSWORD)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 2))
                    .withWidth(ConfigDef.Width.MEDIUM)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "The password for the key store file. "
                                    + "This is optional and only needed if 'database.ssl.keystore' is configured.");

    public static final Field SSL_TRUSTSTORE =
            Field.create("database.ssl.truststore")
                    .withDisplayName("SSL Truststore")
                    .withType(ConfigDef.Type.STRING)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 3))
                    .withWidth(ConfigDef.Width.LONG)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "The location of the trust store file for the server certificate verification.");

    public static final Field SSL_TRUSTSTORE_PASSWORD =
            Field.create("database.ssl.truststore.password")
                    .withDisplayName("SSL Truststore Password")
                    .withType(ConfigDef.Type.PASSWORD)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 4))
                    .withWidth(ConfigDef.Width.MEDIUM)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "The password for the trust store file. "
                                    + "Used to check the integrity of the truststore, and unlock the truststore.");

    public static final Field CONNECTION_TIMEOUT_MS =
            Field.create("connect.timeout.ms")
                    .withDisplayName("Connection Timeout (ms)")
                    .withType(ConfigDef.Type.INT)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "Maximum time to wait after trying to connect to the database before timing out, given in milliseconds. Defaults to 30 seconds (30,000 ms).")
                    .withDefault(30 * 1000)
                    .withValidation(Field::isPositiveInteger);

    public static final Field EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE =
            Field.create("event.deserialization.failure.handling.mode")
                    .withDisplayName("Event deserialization failure handling")
                    .withEnum(
                            EventProcessingFailureHandlingMode.class,
                            EventProcessingFailureHandlingMode.FAIL)
                    .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 21))
                    .withValidation(
                            TiDBConnectorConfig
                                    ::validateEventDeserializationFailureHandlingModeNotSet)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "Specify how failures during deserialization of binlog events (i.e. when encountering a corrupted event) should be handled, including:"
                                    + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                                    + "'warn' the problematic event and its binlog position will be logged and the event will be skipped;"
                                    + "'ignore' the problematic event will be skipped.");

    public static final Field INCONSISTENT_SCHEMA_HANDLING_MODE =
            Field.create("inconsistent.schema.handling.mode")
                    .withDisplayName("Inconsistent schema failure handling")
                    .withEnum(
                            EventProcessingFailureHandlingMode.class,
                            EventProcessingFailureHandlingMode.FAIL)
                    .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 2))
                    .withValidation(
                            TiDBConnectorConfig::validateInconsistentSchemaHandlingModeNotIgnore)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDescription(
                            "Specify how binlog events that belong to a table missing from internal schema representation (i.e. internal representation is not consistent with database) should be handled, including:"
                                    + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                                    + "'warn' the problematic event and its binlog position will be logged and the event will be skipped;"
                                    + "'skip' the problematic event will be skipped.");

    private static int validateEventDeserializationFailureHandlingModeNotSet(
            Configuration config, Field field, Field.ValidationOutput problems) {
        final String modeName =
                config.asMap().get(EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name());
        if (modeName != null) {
            LOGGER.warn(
                    "Configuration option '{}' is renamed to '{}'",
                    EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name(),
                    EVENT_PROCESSING_FAILURE_HANDLING_MODE.name());
            if (EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING.equals(
                    modeName)) {
                LOGGER.warn(
                        "Value '{}' of configuration option '{}' is deprecated and should be replaced with '{}'",
                        EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING,
                        EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name(),
                        EventProcessingFailureHandlingMode.SKIP.getValue());
            }
        }
        return 0;
    }

    private static int validateInconsistentSchemaHandlingModeNotIgnore(
            Configuration config, Field field, Field.ValidationOutput problems) {
        final String modeName = config.getString(INCONSISTENT_SCHEMA_HANDLING_MODE);
        if (EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING.equals(
                modeName)) {
            LOGGER.warn(
                    "Value '{}' of configuration option '{}' is deprecated and should be replaced with '{}'",
                    EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING,
                    INCONSISTENT_SCHEMA_HANDLING_MODE.name(),
                    EventProcessingFailureHandlingMode.SKIP.getValue());
        }
        return 0;
    }
}
