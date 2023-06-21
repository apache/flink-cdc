/*
 * Copyright 2023 Ververica Inc.
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

package com.vervetica.cdc.connectors.vitess;

import org.apache.flink.test.util.AbstractTestBase;

import com.vervetica.cdc.connectors.vitess.container.VitessContainer;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.vervetica.cdc.connectors.vitess.container.VitessContainer.GRPC_PORT;
import static com.vervetica.cdc.connectors.vitess.container.VitessContainer.MYSQL_PORT;
import static com.vervetica.cdc.connectors.vitess.container.VitessContainer.VTCTLD_GRPC_PORT;
import static org.junit.Assert.assertNotNull;

/** Basic class for testing Vitess source, this contains a Vitess container. */
public abstract class VitessTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(VitessTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    protected static final VitessContainer VITESS_CONTAINER =
            (VitessContainer)
                    new VitessContainer()
                            .withKeyspace("test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpwd")
                            .withExposedPorts(MYSQL_PORT, GRPC_PORT, VTCTLD_GRPC_PORT)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(VITESS_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    public Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(VITESS_CONTAINER.getJdbcUrl());
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = VitessTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
