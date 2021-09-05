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

package com.ververica.cdc.connectors.mysql.testutils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Docker container for MySQL. The difference between this class and {@link
 * org.testcontainers.containers.MySQLContainer} is that TC MySQLContainer has problems when
 * overriding mysql conf file, i.e. my.cnf.
 */
@SuppressWarnings("rawtypes")
public class MySqlContainer extends JdbcDatabaseContainer {

    public static final String IMAGE = "mysql";
    public static final Integer MYSQL_PORT = 3306;

    private static final String MY_CNF_CONFIG_OVERRIDE_PARAM_NAME = "MY_CNF";
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";
    private static final String MYSQL_ROOT_USER = "root";
    private static final String GTID_MOD = "gtid-mode";
    private static final String ENFORCE_GTID_CONSISTENCY = "enforce-gtid-consistency";
    private static final String LOG_SLAVE_UPDATES = "log-slave-updates";
    private static final String LOG_BIN = "log_bin";

    private String databaseName = "test";
    private String username = "test";
    private String password = "test";

    private boolean enableGtid;
    private MysqlVersion version;

    public MySqlContainer() {
        this(MysqlVersion.V5_7);
    }

    public MySqlContainer(MysqlVersion version) {
        super(IMAGE + ":" + version.getVersion());
        addExposedPort(MYSQL_PORT);
        this.version = version;
    }

    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(MYSQL_PORT));
    }

    @Override
    protected void configure() {
        String myCnf = (String) parameters.get(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME);
        if (StringUtils.isBlank(myCnf)) {
            throw new IllegalStateException(
                    "Need to set 'MY_CNF' param through withConfigurationOverride");
        }

        // Set gtid config param for my.conf
        String newConfigFile = buildNewMySqlConfigFile(myCnf);
        logger().info("New mysql config file={}", newConfigFile);
        logger().info("New mysql config content={}", readFileContent(newConfigFile));
        withConfigurationOverride(newConfigFile);

        optionallyMapResourceParameterAsVolume(
                MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, "/etc/mysql/", "mysql-default-conf");

        if (parameters.containsKey(SETUP_SQL_PARAM_NAME)) {
            optionallyMapResourceParameterAsVolume(
                    SETUP_SQL_PARAM_NAME, "/docker-entrypoint-initdb.d/", "N/A");
        }

        addEnv("MYSQL_DATABASE", databaseName);
        addEnv("MYSQL_USER", username);
        if (password != null && !password.isEmpty()) {
            addEnv("MYSQL_PASSWORD", password);
            addEnv("MYSQL_ROOT_PASSWORD", password);
        } else if (MYSQL_ROOT_USER.equalsIgnoreCase(username)) {
            addEnv("MYSQL_ALLOW_EMPTY_PASSWORD", "yes");
        } else {
            throw new ContainerLaunchException(
                    "Empty password can be used only with the root user");
        }
        setStartupAttempts(3);
    }

    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(MYSQL_PORT);
    }

    @Override
    protected String constructUrlForConnection(String queryString) {
        String url = super.constructUrlForConnection(queryString);

        if (!url.contains("useSSL=")) {
            String separator = url.contains("?") ? "&" : "?";
            url = url + separator + "useSSL=false";
        }

        if (!url.contains("allowPublicKeyRetrieval=")) {
            url = url + "&allowPublicKeyRetrieval=true";
        }

        return url;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @SuppressWarnings("unchecked")
    public MySqlContainer withConfigurationOverride(String s) {
        parameters.put(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, s);
        return this;
    }

    @SuppressWarnings("unchecked")
    public MySqlContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    @Override
    public MySqlContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public MySqlContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    @Override
    public MySqlContainer withPassword(final String password) {
        this.password = password;
        return this;
    }

    public MySqlContainer withEnableGtid(final boolean enableGtid) {
        this.enableGtid = enableGtid;
        return this;
    }

    public Boolean isEnableGtid() {
        return enableGtid;
    }

    public MysqlVersion getVersion() {
        return version;
    }

    /** Mysql version emum. */
    public enum MysqlVersion {
        V5_5("5.5"),
        V5_6("5.6"),
        V5_7("5.7"),
        V8_0("8.0");
        private String version;

        MysqlVersion(String version) {
            this.version = version;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "MysqlVersion{" + "version='" + version + '\'' + '}';
        }
    }

    //
    // ---------------------------------------------------------------------------------------------
    //
    private String buildNewMySqlConfigFile(String myCnf) {
        try {
            // gtid-mod=ON
            // This differs from other enumeration types but maintains compatibility with the
            // boolean type used in previous versions
            // enforce-gtid-consistency=1

            // Prior to MySQL 5.7.5, starting the server with --gtid-mode=ON required that the
            // server also be started with the --log-bin and --log-slave-updates options
            // See
            // https://dev.mysql.com/doc/refman/5.7/en/replication-options-gtids.html#sysvar_gtid_mode
            // log_bin=mysql-bin
            // log-slave-updates=ON
            Map<String, String> paramMap = new LinkedHashMap<>();
            final MountableFile mountableFile = MountableFile.forClasspathResource(myCnf);
            List<String> lines =
                    FileUtils.readLines(new File(mountableFile.getFilesystemPath()), "utf-8");
            lines.forEach(
                    line -> {
                        if (line.startsWith("#")) {
                            return;
                        }
                        String[] kv = line.split("=", 2);
                        if (kv.length == 1) {
                            paramMap.put(kv[0].trim(), "");
                        } else {
                            paramMap.put(kv[0].trim(), kv[1].trim());
                        }
                    });

            // mysql 5.5 not support gtid param
            if (version != MysqlVersion.V5_5) {
                if (!enableGtid) {
                    paramMap.put(GTID_MOD, "OFF");
                } else {
                    paramMap.put(GTID_MOD, "ON");
                    paramMap.put(ENFORCE_GTID_CONSISTENCY, "1");
                    if (version == MysqlVersion.V5_6) {
                        paramMap.put(LOG_SLAVE_UPDATES, "ON");
                        if (!paramMap.containsKey(LOG_BIN)) {
                            paramMap.put(LOG_BIN, "mysql-bin");
                        }
                    }
                }
            }

            StringBuilder configResults = new StringBuilder();
            paramMap.entrySet()
                    .forEach(
                            e -> {
                                if (StringUtils.isBlank(e.getValue())) {
                                    configResults.append(e.getKey());
                                } else {
                                    configResults.append(e.getKey() + "=" + e.getValue());
                                }
                                configResults.append("\n");
                            });

            String finalConfig = buildMySqlConfigFile(configResults.toString());
            return finalConfig;
        } catch (IOException e) {
            throw new RuntimeException("Build gtid config file failed", e);
        }
    }

    private String buildMySqlConfigFile(String content) {
        try {
            File resourceFolder =
                    Paths.get(
                                    Objects.requireNonNull(
                                                    MySqlContainer.class
                                                            .getClassLoader()
                                                            .getResource("."))
                                            .toURI())
                            .toFile();
            TemporaryFolder tempFolder = new TemporaryFolder(resourceFolder);
            tempFolder.create();
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(Paths.get(folder.getPath(), "my.cnf"));
            Files.write(
                    cnf,
                    Collections.singleton(content),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }

    private String readFileContent(String file) {
        try {
            return FileUtils.readFileToString(
                    new File(MountableFile.forClasspathResource(file).getFilesystemPath()),
                    "utf-8");
        } catch (IOException e) {
            logger().error("Read file error", e);
        }
        return "";
    }
}
