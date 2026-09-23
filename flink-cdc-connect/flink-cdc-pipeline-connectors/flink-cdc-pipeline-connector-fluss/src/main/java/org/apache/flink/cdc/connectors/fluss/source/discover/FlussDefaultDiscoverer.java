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

package org.apache.flink.cdc.connectors.fluss.source.discover;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The default {@link TableDiscoverer} for Fluss that subscribes to tables whose fully-qualified
 * name (formatted as {@code "database.tableName"}) matches a user-provided Java regular expression.
 *
 * <p>The pattern is matched against the fully-qualified name of every table across every database
 * visible to the Fluss cluster. A single regex can therefore span multiple databases, e.g. {@code
 * "source_db\\..*|audit_db\\.events_.*"}.
 *
 * <p>Note: dots in database or table names must be escaped (e.g. {@code "db\\.table"}) because
 * {@code .} is a regex meta-character.
 *
 * <p><b>Configuration keys</b> (read from the full connector configuration):
 *
 * <ul>
 *   <li>{@code bootstrap.servers} — Fluss bootstrap servers (required).
 *   <li>{@code table.discoverer.pattern} — Java regex for matching fully-qualified table names
 *       (required).
 * </ul>
 */
public class FlussDefaultDiscoverer implements TableDiscoverer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlussDefaultDiscoverer.class);

    public static final ConfigOption<String> PATTERN =
            ConfigOptions.key("table.discoverer.pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A Java regular expression matched against the fully-qualified "
                                    + "'database.tableName' of every visible Fluss table. "
                                    + "Required when table.discoverer.type is 'fluss-default'. "
                                    + "Example: 'source_db\\..*|audit_db\\.events_.*'.");

    private transient Connection connection;
    private transient Admin admin;
    private transient String pattern;

    @Override
    public void open(Context context) throws Exception {
        Configuration config = context.getConfiguration();

        pattern = config.get(PATTERN);
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException(
                    "'"
                            + PATTERN.key()
                            + "' is required when 'table.discoverer.type' is 'fluss-default'.");
        }

        String bootstrapServers =
                config.get(
                        ConfigOptions.key("bootstrap.servers")
                                .stringType()
                                .noDefaultValue()
                                .withDescription("Fluss bootstrap servers."));
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException(
                    "'bootstrap.servers' is required for FlussDefaultDiscoverer.");
        }

        org.apache.fluss.config.Configuration flussConfig =
                new org.apache.fluss.config.Configuration();
        flussConfig.setString(
                org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        // Also propagate any properties.client.* options
        config.toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("properties.client.")) {
                                flussConfig.setString(key.substring("properties.".length()), value);
                            }
                        });

        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
        LOG.info(
                "FlussDefaultDiscoverer opened connection to '{}', pattern='{}'.",
                bootstrapServers,
                pattern);
    }

    @Override
    public Set<TableId> discover() throws Exception {
        Pattern compiled = Pattern.compile("^" + pattern + "$");

        Set<TableId> matched = new LinkedHashSet<>();
        List<String> databases = admin.listDatabases().get();
        for (String database : databases) {
            List<String> tables = admin.listTables(database).get();
            for (String tableName : tables) {
                String fqn = database + "." + tableName;
                if (compiled.matcher(fqn).matches()) {
                    matched.add(TableId.tableId(database, tableName));
                }
            }
        }
        LOG.debug(
                "FlussDefaultDiscoverer discovered {} tables with pattern '{}'.",
                matched.size(),
                pattern);
        return matched;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
            LOG.info("FlussDefaultDiscoverer closed Fluss connection.");
        }
    }

    /** Converts a {@link TableId} (two-part: schemaName.tableName) to a Fluss {@link TablePath}. */
    public static TablePath toTablePath(TableId tableId) {
        return new TablePath(tableId.getSchemaName(), tableId.getTableName());
    }
}
