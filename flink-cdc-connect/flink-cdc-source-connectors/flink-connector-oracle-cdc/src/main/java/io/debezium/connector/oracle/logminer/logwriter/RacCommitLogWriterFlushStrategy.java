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

package io.debezium.connector.oracle.logminer.logwriter;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Oracle RAC LogWriter flush strategy with service-name compatible JDBC URL rewriting.
 *
 * <p>Debezium 1.9.8 recreates RAC node connections by only overriding hostname/port. That path
 * works for SID-style URLs, but it can generate invalid JDBC connect strings when the connector
 * uses service-name URLs such as {@code jdbc:oracle:thin:@//host:port/service}. This local copy
 * preserves the upstream behavior while rebuilding a node-specific URL for each RAC flush
 * connection.
 */
public class RacCommitLogWriterFlushStrategy implements LogWriterFlushStrategy {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RacCommitLogWriterFlushStrategy.class);
    private static final String JDBC_URL_KEY = "url";
    private static final Pattern SERVICE_NAME_URL_PATTERN =
            Pattern.compile(
                    "^(jdbc:oracle:[^:]+:@//)([^/:]+):(\\d+)(/.*)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SID_URL_PATTERN =
            Pattern.compile(
                    "^(jdbc:oracle:[^:]+:@)([^/:]+):(\\d+)(:.*)$", Pattern.CASE_INSENSITIVE);

    private final Map<String, CommitLogWriterFlushStrategy> flushStrategies = new HashMap<>();
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final JdbcConfiguration jdbcConfiguration;
    private final Set<String> hosts;
    private final String connectorRawJdbcUrl;
    private final String connectorDatabaseName;
    private final String connectorUsername;
    private final String connectorPassword;

    public RacCommitLogWriterFlushStrategy(
            OracleConnectorConfig connectorConfig,
            JdbcConfiguration jdbcConfig,
            OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConfiguration = jdbcConfig;
        this.streamingMetrics = streamingMetrics;
        this.hosts =
                connectorConfig.getRacNodes().stream()
                        .map(String::toUpperCase)
                        .collect(Collectors.toSet());
        this.connectorRawJdbcUrl = connectorConfig.getConfig().getString(OracleConnectorConfig.URL);
        this.connectorDatabaseName = connectorConfig.getDatabaseName();
        this.connectorUsername = connectorConfig.getJdbcConfig().getUser();
        this.connectorPassword = connectorConfig.getJdbcConfig().getPassword();
        recreateRacNodeFlushStrategies();
    }

    @Override
    public void close() {
        closeRacNodeFlushStrategies();
        flushStrategies.clear();
    }

    @Override
    public String getHost() {
        return String.join(", ", hosts);
    }

    @Override
    public void flush(Scn currentScn) throws InterruptedException {
        Instant startTime = Instant.now();
        if (flushStrategies.isEmpty()) {
            throw new DebeziumException("No RAC node addresses supplied or currently connected");
        }

        boolean recreateConnections = false;
        for (Map.Entry<String, CommitLogWriterFlushStrategy> entry : flushStrategies.entrySet()) {
            final CommitLogWriterFlushStrategy strategy = entry.getValue();
            try {
                strategy.flush(currentScn);
            } catch (Exception e) {
                LOGGER.warn("Failed to flush LGWR buffer on RAC node '{}'", strategy.getHost(), e);
                recreateConnections = true;
            }
        }

        if (recreateConnections) {
            recreateRacNodeFlushStrategies();
            LOGGER.warn(
                    "Not all LGWR buffers were flushed, waiting 3 seconds for Oracle to flush automatically.");
            Metronome metronome = Metronome.sleeper(Duration.ofSeconds(3), Clock.SYSTEM);
            try {
                metronome.pause();
            } catch (InterruptedException e) {
                LOGGER.warn("The LGWR buffer wait was interrupted.");
                throw e;
            }
        }

        LOGGER.trace("LGWR flush took {} to complete.", Duration.between(startTime, Instant.now()));
    }

    private void recreateRacNodeFlushStrategies() {
        closeRacNodeFlushStrategies();
        flushStrategies.clear();

        for (String hostName : hosts) {
            try {
                final String[] parts = hostName.split(":");
                flushStrategies.put(
                        hostName, createHostFlushStrategy(parts[0], Integer.parseInt(parts[1])));
            } catch (SQLException e) {
                throw new DebeziumException("Cannot connect to RAC node '" + hostName + "'", e);
            }
        }
    }

    private CommitLogWriterFlushStrategy createHostFlushStrategy(String hostName, Integer port)
            throws SQLException {
        JdbcConfiguration jdbcHostConfig =
                buildHostJdbcConfiguration(
                        jdbcConfiguration,
                        connectorRawJdbcUrl,
                        connectorDatabaseName,
                        connectorUsername,
                        connectorPassword,
                        hostName,
                        port);
        LOGGER.debug(
                "Creating flush connection to RAC node '{}' with jdbc url '{}' user='{}' passwordPresent={}",
                hostName,
                jdbcHostConfig.getString(JDBC_URL_KEY),
                jdbcHostConfig.getUser(),
                !Strings.isNullOrEmpty(jdbcHostConfig.getPassword()));
        return new CommitLogWriterFlushStrategy(jdbcHostConfig);
    }

    static JdbcConfiguration buildHostJdbcConfiguration(
            JdbcConfiguration jdbcConfiguration, String hostName, Integer port) {
        return buildHostJdbcConfiguration(
                jdbcConfiguration, null, null, null, null, hostName, port);
    }

    static JdbcConfiguration buildHostJdbcConfiguration(
            JdbcConfiguration jdbcConfiguration,
            String rawJdbcUrl,
            String databaseName,
            String username,
            String password,
            String hostName,
            Integer port) {
        JdbcConfiguration.Builder builder =
                JdbcConfiguration.copy(jdbcConfiguration)
                        .with(JdbcConfiguration.HOSTNAME, hostName)
                        .with(JdbcConfiguration.PORT, port);
        if (!Strings.isNullOrEmpty(username)) {
            builder.with(JdbcConfiguration.USER, username);
        }
        if (!Strings.isNullOrEmpty(password)) {
            builder.with(JdbcConfiguration.PASSWORD, password);
        }
        String jdbcUrl =
                resolveJdbcUrl(jdbcConfiguration, rawJdbcUrl, databaseName, hostName, port);
        if (!Strings.isNullOrEmpty(jdbcUrl)) {
            builder.with(JDBC_URL_KEY, jdbcUrl);
        }
        return JdbcConfiguration.adapt(builder.build());
    }

    static String resolveJdbcUrl(
            JdbcConfiguration jdbcConfiguration, String hostName, Integer port) {
        return resolveJdbcUrl(jdbcConfiguration, null, null, hostName, port);
    }

    static String resolveJdbcUrl(
            JdbcConfiguration jdbcConfiguration,
            String rawJdbcUrl,
            String databaseName,
            String hostName,
            Integer port) {
        if (!Strings.isNullOrEmpty(rawJdbcUrl)) {
            return rewriteJdbcUrl(rawJdbcUrl, hostName, port);
        }

        String jdbcUrl = jdbcConfiguration.getString(JDBC_URL_KEY);
        if (!Strings.isNullOrEmpty(jdbcUrl)) {
            return rewriteJdbcUrl(jdbcUrl, hostName, port);
        }

        if (!Strings.isNullOrEmpty(databaseName)) {
            return "jdbc:oracle:thin:@//" + hostName + ":" + port + "/" + databaseName;
        }

        String fallbackDatabase = jdbcConfiguration.getString(JdbcConfiguration.DATABASE);
        if (!Strings.isNullOrEmpty(fallbackDatabase)) {
            return "jdbc:oracle:thin:@//" + hostName + ":" + port + "/" + fallbackDatabase;
        }

        return null;
    }

    static String rewriteJdbcUrl(String jdbcUrl, String hostName, Integer port) {
        Matcher serviceNameMatcher = SERVICE_NAME_URL_PATTERN.matcher(jdbcUrl);
        if (serviceNameMatcher.matches()) {
            return serviceNameMatcher.group(1)
                    + hostName
                    + ":"
                    + port
                    + serviceNameMatcher.group(4);
        }

        Matcher sidMatcher = SID_URL_PATTERN.matcher(jdbcUrl);
        if (sidMatcher.matches()) {
            return sidMatcher.group(1) + hostName + ":" + port + sidMatcher.group(4);
        }

        return jdbcUrl;
    }

    private void closeRacNodeFlushStrategies() {
        for (CommitLogWriterFlushStrategy strategy : flushStrategies.values()) {
            try {
                strategy.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close RAC connection to node '{}'", strategy.getHost(), e);
                streamingMetrics.incrementWarningCount();
            }
        }
    }
}
