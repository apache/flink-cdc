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

package org.apache.flink.cdc.connectors.db2.factory;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;

/** Testcontainers base for DB2 pipeline connector tests. */
class PipelineDb2TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineDb2TestBase.class);

    private static final DockerImageName DEBEZIUM_DOCKER_IMAGE_NAME =
            DockerImageName.parse(
                            new ImageFromDockerfile("custom/db2-cdc:1.4")
                                    .withDockerfile(
                                            createDb2ServerBuildContext().resolve("Dockerfile"))
                                    .get())
                    .asCompatibleSubstituteFor("ibmcom/db2");

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final CompletableFuture<Void> db2AsnAgentStarted = new CompletableFuture<>();

    protected static final Db2Container DB2_CONTAINER =
            new Db2Container(DEBEZIUM_DOCKER_IMAGE_NAME)
                    .withDatabaseName("testdb")
                    .withUsername("db2inst1")
                    .withPassword("flinkpw")
                    .withEnv("AUTOCONFIG", "false")
                    .withEnv("ARCHIVE_LOGS", "true")
                    .acceptLicense()
                    .withCreateContainerCmdModifier(
                            createContainerCmd -> createContainerCmd.withPlatform("linux/amd64"))
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withLogConsumer(
                            outputFrame -> {
                                if (outputFrame
                                        .getUtf8String()
                                        .contains("The asncdc program enable finished")) {
                                    db2AsnAgentStarted.complete(null);
                                }
                            });

    @BeforeAll
    static void startContainers() {
        LOG.info("Starting DB2 container...");
        Startables.deepStart(Stream.of(DB2_CONTAINER)).join();
        LOG.info("DB2 container is started.");

        db2AsnAgentStarted.join();
        assertCdcAgentRunning();
        LOG.info("DB2 ASN agent is available.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping DB2 container...");
        DB2_CONTAINER.stop();
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                DB2_CONTAINER.getJdbcUrl(),
                DB2_CONTAINER.getUsername(),
                DB2_CONTAINER.getPassword());
    }

    private static void assertCdcAgentRunning() {
        try (Connection connection =
                        DriverManager.getConnection(
                                DB2_CONTAINER.getJdbcUrl(),
                                DB2_CONTAINER.getUsername(),
                                DB2_CONTAINER.getPassword());
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("VALUES ASNCDC.ASNCDCSERVICES('status','asncdc')")) {
            Assertions.assertThat(resultSet.next()).isTrue();
            Assertions.assertThat(resultSet.getString(1))
                    .doesNotContainIgnoringCase("asncap is not running");
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to verify DB2 ASN agent status.", e);
        }
    }

    protected void initializeDb2Table(String sqlFile, String tableName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            if (checkTableExists(connection, tableName)) {
                dropTestTable(connection, tableName.toUpperCase(Locale.ROOT));
                Thread.sleep(10_000);
            }
            for (String stmt : readSqlStatements(sqlFile)) {
                statement.execute(stmt);
                Thread.sleep(500);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        }
        return format("(%s)", StringUtils.join(captureCustomerTables, ","));
    }

    private static List<String> readSqlStatements(String sqlFile) throws IOException {
        String ddlFile = String.format("db2_server/%s.sql", sqlFile);
        InputStream ddlStream =
                PipelineDb2TestBase.class.getClassLoader().getResourceAsStream(ddlFile);
        Assertions.assertThat(ddlStream).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(ddlStream, StandardCharsets.UTF_8))) {
            return Arrays.stream(
                            reader.lines()
                                    .map(String::trim)
                                    .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                    .map(
                                            x -> {
                                                Matcher matcher = COMMENT_PATTERN.matcher(x);
                                                return matcher.matches() ? matcher.group(1) : x;
                                            })
                                    .collect(Collectors.joining("\n"))
                                    .split(";"))
                    .filter(stmt -> !stmt.trim().isEmpty())
                    .collect(Collectors.toList());
        }
    }

    private static void dropTestTable(Connection connection, String tableName) {
        try {
            Awaitility.await(String.format("cdc remove table %s", tableName))
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    connection
                                            .createStatement()
                                            .execute(
                                                    String.format(
                                                            "CALL ASNCDC.REMOVETABLE('DB2INST1', '%s')",
                                                            tableName));
                                    connection
                                            .createStatement()
                                            .execute(
                                                    "VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc')");
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            "CDC remove table {} failed, will retry.",
                                            tableName,
                                            e);
                                    return false;
                                }
                            });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to remove CDC table " + tableName, e);
        }

        try {
            Awaitility.await(String.format("drop table %s", tableName))
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    connection
                                            .createStatement()
                                            .execute(
                                                    String.format(
                                                            "DROP TABLE DB2INST1.%s", tableName));
                                    connection.commit();
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn("Drop table {} failed, will retry.", tableName, e);
                                    return false;
                                }
                            });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to drop table " + tableName, e);
        }
    }

    private static boolean checkTableExists(Connection connection, String tableName) {
        AtomicBoolean tableExists = new AtomicBoolean(false);
        try {
            Awaitility.await(String.format("check table %s exists", tableName))
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    ResultSet resultSet =
                                            connection
                                                    .createStatement()
                                                    .executeQuery(
                                                            String.format(
                                                                    "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABNAME = '%s' AND TABSCHEMA = 'DB2INST1'",
                                                                    tableName));
                                    if (resultSet.next() && resultSet.getInt(1) == 1) {
                                        tableExists.set(true);
                                    }
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            "Check table {} exists failed, will retry.",
                                            tableName,
                                            e);
                                    return false;
                                }
                            });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to check table " + tableName, e);
        }
        return tableExists.get();
    }

    private static Path createDb2ServerBuildContext() {
        Path sourceDir = getDb2ServerResourceDir();
        try {
            Path targetDir = Files.createTempDirectory("flink-cdc-db2-server-build");
            try (Stream<Path> files = Files.walk(sourceDir)) {
                for (Path source : files.collect(Collectors.toList())) {
                    Path target = targetDir.resolve(sourceDir.relativize(source).toString());
                    if (Files.isDirectory(source)) {
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(source, target);
                    }
                }
            }
            Path dockerfile = targetDir.resolve("Dockerfile");
            String dockerfileContent =
                    new String(Files.readAllBytes(dockerfile), StandardCharsets.UTF_8);
            Files.write(
                    dockerfile,
                    dockerfileContent
                            .replace(
                                    "FROM ibmcom/db2:11.5.0.0a",
                                    "FROM --platform=linux/amd64 ibmcom/db2:11.5.0.0a")
                            .getBytes(StandardCharsets.UTF_8));
            return targetDir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create DB2 Docker build context", e);
        }
    }

    private static Path getDb2ServerResourceDir() {
        try {
            URL dockerfile =
                    PipelineDb2TestBase.class.getClassLoader().getResource("db2_server/Dockerfile");
            Assertions.assertThat(dockerfile)
                    .withFailMessage("Cannot locate db2_server/Dockerfile")
                    .isNotNull();
            if ("file".equals(dockerfile.getProtocol())) {
                return Paths.get(dockerfile.toURI()).getParent();
            }
            if ("jar".equals(dockerfile.getProtocol())) {
                return extractDb2ServerResources((JarURLConnection) dockerfile.openConnection());
            }
            throw new IllegalStateException("Unsupported resource protocol: " + dockerfile);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to resolve db2_server resources", e);
        }
    }

    private static Path extractDb2ServerResources(JarURLConnection jarConnection)
            throws IOException {
        Path tempDir = Files.createTempDirectory("flink-cdc-db2-server");
        Path db2ServerDir = tempDir.resolve("db2_server");
        Files.createDirectories(db2ServerDir);
        try (JarFile jarFile = jarConnection.getJarFile()) {
            for (JarEntry entry : jarFile.stream().collect(Collectors.toList())) {
                String name = entry.getName();
                if (!entry.isDirectory() && name.startsWith("db2_server/")) {
                    Path target = tempDir.resolve(name);
                    Files.createDirectories(target.getParent());
                    try (InputStream inputStream = jarFile.getInputStream(entry)) {
                        Files.copy(inputStream, target);
                    }
                }
            }
        }
        return db2ServerDir;
    }
}
