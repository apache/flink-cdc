package org.apache.flink.cdc.connectors.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end tests for GaussDB CDC connector targeting a remote instance. */
class GaussDbE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDbE2eITCase.class);

    // Remote GaussDB configuration
    private static final String GAUSS_HOSTNAME = "10.250.0.51";
    private static final int GAUSS_PORT = 8000;
    private static final String GAUSS_USERNAME = "tom";
    private static final String GAUSS_PASSWORD = "Gauss_235";
    private static final String GAUSS_DATABASE = "db1";
    private static final String GAUSS_SCHEMA = "public";

    protected static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String FIXED_SLOT_NAME = "flink_gaussdb_e2e_slot";

    private static final Path gaussdbCdcJar = TestUtils.getResource("gaussdb-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
    private static final Path gaussdbDriverJar = TestUtils.getResource("gaussdbjdbc.jar");

    @BeforeEach
    public void before() {
        super.before();
        cleanupReplicationSlots();
        initializeGaussDBTable();
    }

    private void cleanupReplicationSlots() {
        LOG.info("Cleaning up orphaned replication slots...");
        try (Connection conn = getGaussJdbcConnection();
                Statement statement = conn.createStatement()) {
            List<String> orphanedSlots = new ArrayList<>();
            try (ResultSet rs =
                    statement.executeQuery(
                            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'flink_e2e_%' OR slot_name = '"
                                    + FIXED_SLOT_NAME
                                    + "'")) {
                while (rs.next()) {
                    orphanedSlots.add(rs.getString(1));
                }
            }

            for (String slot : orphanedSlots) {
                try {
                    LOG.info("Dropping slot: {}", slot);
                    statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", slot));
                } catch (SQLException e) {
                    LOG.warn("Failed to drop slot {}", slot, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup replication slots", e);
        }
    }

    private void initializeGaussDBTable() {
        try (Connection conn = getGaussJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS public.products_gauss;");
            statement.execute(
                    "CREATE TABLE public.products_gauss ("
                            + " id SERIAL PRIMARY KEY,"
                            + " name VARCHAR(255),"
                            + " description VARCHAR(512),"
                            + " weight DECIMAL(10,3)"
                            + ");");
            statement.execute(
                    "INSERT INTO public.products_gauss VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14);");
            statement.execute(
                    "INSERT INTO public.products_gauss VALUES (102, 'car battery', '12V car battery', 8.1);");
        } catch (Exception e) {
            LOG.error("Failed to initialize GaussDB table", e);
            throw new RuntimeException("Failed to initialize GaussDB table", e);
        }
    }

    private Connection getGaussJdbcConnection() throws SQLException, ClassNotFoundException {
        Class.forName(PG_DRIVER_CLASS);
        String url =
                String.format(
                        "jdbc:postgresql://%s:%d/%s?sslmode=disable",
                        GAUSS_HOSTNAME, GAUSS_PORT, GAUSS_DATABASE);
        return DriverManager.getConnection(url, GAUSS_USERNAME, GAUSS_PASSWORD);
    }

    @Test
    void testGaussDbCdc() throws Exception {
        List<String> sourceSql =
                Arrays.asList(
                        "CREATE TABLE products_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'gaussdb-cdc',",
                        " 'hostname' = '" + GAUSS_HOSTNAME + "',",
                        " 'port' = '" + GAUSS_PORT + "',",
                        " 'username' = '" + GAUSS_USERNAME + "',",
                        " 'password' = '" + GAUSS_PASSWORD + "',",
                        " 'database-name' = '" + GAUSS_DATABASE + "',",
                        " 'schema-name' = '" + GAUSS_SCHEMA + "',",
                        " 'table-name' = 'products_gauss',",
                        " 'slot.name' = '" + FIXED_SLOT_NAME + "',",
                        " 'decoding.plugin.name' = 'mppdb_decoding',",
                        " 'scan.incremental.snapshot.enabled' = 'true'",
                        ");");

        List<String> sinkSql =
                Arrays.asList(
                        "CREATE TABLE products_sink (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:3306/%s',",
                                INTER_CONTAINER_MYSQL_ALIAS,
                                mysqlInventoryDatabase.getDatabaseName()),
                        " 'table-name' = 'products_sink',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "'",
                        ");",
                        "INSERT INTO products_sink",
                        "SELECT * FROM products_source;");

        List<String> sqlLines =
                Stream.concat(sourceSql.stream(), sinkSql.stream()).collect(Collectors.toList());

        // Submit job
        submitSQLJob(sqlLines, gaussdbCdcJar, jdbcJar, mysqlDriverJar, gaussdbDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(60));

        LOG.info("Waiting for Flink to settle...");
        Thread.sleep(30000);

        // Update data
        LOG.info("Updating data in GaussDB...");
        try (Connection conn = getGaussJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "UPDATE public.products_gauss SET description='Heavy duty car battery' WHERE id=102;");
            statement.execute(
                    "INSERT INTO public.products_gauss VALUES (103, 'drill', 'Electric drill', 1.5);");
        }

        // Verify results
        String mysqlUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());

        List<String> expectResult =
                new ArrayList<>(
                        Arrays.asList(
                                "101,scooter,Small 2-wheel scooter,3.140",
                                "102,car battery,Heavy duty car battery,8.100",
                                "103,drill,Electric drill,1.500"));

        long endTimeout = System.currentTimeMillis() + 90000L;
        boolean resultMatch = false;
        List<String> actualResults = null;

        while (System.currentTimeMillis() < endTimeout) {
            actualResults =
                    getActualResults(
                            mysqlUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, MYSQL_DRIVER_CLASS);
            System.out.println("DEBUG: Actual results: " + actualResults);

            if (actualResults.size() == expectResult.size()) {
                List<String> sortedActual = new ArrayList<>(actualResults);
                List<String> sortedExpect = new ArrayList<>(expectResult);
                java.util.Collections.sort(sortedActual);
                java.util.Collections.sort(sortedExpect);

                if (sortedActual.equals(sortedExpect)) {
                    resultMatch = true;
                    break;
                }
            }
            Thread.sleep(5000L);
        }

        if (!resultMatch) {
            System.err.println("DEBUG FAILURE: Results do not match!");
            System.err.println("DEBUG FAILURE: Expected: " + expectResult);
            System.err.println("DEBUG FAILURE: Actual: " + actualResults);
            printContainerLogFiles();
            org.junit.jupiter.api.Assertions.assertEquals(expectResult, actualResults);
        }
    }

    private void printContainerLogFiles() {
        try {
            System.out.println("========== JobManager Logs ==========");
            System.out.println(jobManager.getLogs());
            ExecResult jmLogs =
                    jobManager.execInContainer(
                            "cat", "/opt/flink/log/flink--standalonesession-0-jobmanager.log");
            System.out.println(jmLogs.getStdout());

            System.out.println("========== TaskManager Logs ==========");
            System.out.println(taskManager.getLogs());
            ExecResult tmLogs =
                    taskManager.execInContainer(
                            "cat", "/opt/flink/log/flink--taskexecutor-0-taskmanager.log");
            System.out.println(tmLogs.getStdout());
        } catch (Exception e) {
            System.err.println("Failed to print container logs: " + e.getMessage());
        }
    }

    private List<String> getActualResults(String url, String user, String password, String driver)
            throws Exception {
        Class.forName(driver);
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT id, name, description, weight FROM products_sink")) {
            List<String> results = new ArrayList<>();
            while (rs.next()) {
                results.add(
                        String.format(
                                "%d,%s,%s,%.3f",
                                rs.getInt(1),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getBigDecimal(4)));
            }
            return results;
        }
    }
}
