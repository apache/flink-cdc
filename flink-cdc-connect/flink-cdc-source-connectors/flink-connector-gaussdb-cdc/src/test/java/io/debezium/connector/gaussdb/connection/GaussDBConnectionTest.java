/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb.connection;

import org.apache.flink.cdc.connectors.base.relational.connection.ConnectionPoolId;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPools;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBConnectionPoolFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GaussDBConnectionTest {

    @AfterEach
    void cleanupPools() throws Exception {
        // JdbcConnectionPools is a singleton shared across tests; clear it to avoid leaking pools.
        JdbcConnectionPools.getInstance(new GaussDBConnectionPoolFactory()).clear();
    }

    @Test
    void connectsSuccessfullyAndPassesHealthCheck() throws Exception {
        assertThat(GaussDBConnection.DRIVER_CLASS_NAME).isEqualTo("com.huawei.gaussdb.jdbc.Driver");

        AtomicInteger attempts = new AtomicInteger();
        List<String> executedSql = Collections.synchronizedList(new ArrayList<>());
        ConnectionFactory delegateFactory =
                ignoredConfig -> {
                    attempts.incrementAndGet();
                    return connectionProxy(executedSql);
                };

        JdbcConfiguration config =
                JdbcConfiguration.create()
                        .withHostname("localhost")
                        .withPort(8000)
                        .withDatabase("test_db")
                        .withUser("user")
                        .withPassword("password")
                        .withConnectionTimeoutMs(30_000)
                        .with(GaussDBConnection.CONNECT_MAX_RETRIES_KEY, "3")
                        .with(GaussDBConnection.CONNECT_RETRY_DELAY_MS_KEY, "0")
                        .build();

        GaussDBConnection connection = new GaussDBConnection(config, "test", delegateFactory);
        connection.connect();
        assertThat(connection.isValid()).isTrue();

        assertThat(attempts.get()).isEqualTo(1);
        assertThat(executedSql).contains("SELECT 1");
    }

    @Test
    void retriesConnectionThreeTimesAndThenThrows() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        ConnectionFactory delegateFactory =
                ignoredConfig -> {
                    attempts.incrementAndGet();
                    throw new SQLException("Simulated connection failure");
                };

        JdbcConfiguration config =
                JdbcConfiguration.create()
                        .withHostname("localhost")
                        .withPort(8000)
                        .withDatabase("test_db")
                        .withUser("user")
                        .withPassword("password")
                        .withConnectionTimeoutMs(30_000)
                        .with(GaussDBConnection.CONNECT_MAX_RETRIES_KEY, "3")
                        .with(GaussDBConnection.CONNECT_RETRY_DELAY_MS_KEY, "0")
                        .build();

        GaussDBConnection connection = new GaussDBConnection(config, "test", delegateFactory);

        assertThatThrownBy(connection::connect).isInstanceOf(SQLException.class);
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void managesConnectionPoolLifecycle() throws Exception {
        TestHikariDriver.reset();

        GaussDBConnectionPoolFactory poolFactory = new GaussDBConnectionPoolFactory();
        GaussDBSourceConfig sourceConfig = sourceConfigWithDriver(TestHikariDriver.class.getName());

        JdbcConfiguration jdbcConfig =
                JdbcConfiguration.create()
                        .withHostname(sourceConfig.getHostname())
                        .withPort(sourceConfig.getPort())
                        .withDatabase(sourceConfig.getDatabaseList().get(0))
                        .withUser(sourceConfig.getUsername())
                        .withPassword(sourceConfig.getPassword())
                        .build();

        ConnectionPoolId poolId =
                poolFactory.getPoolId(jdbcConfig, poolFactory.getClass().getName());

        JdbcConnectionPools pools = JdbcConnectionPools.getInstance(poolFactory);
        com.zaxxer.hikari.HikariDataSource dataSource =
                pools.getOrCreateConnectionPool(poolId, sourceConfig);

        assertThat(dataSource.isClosed()).isFalse();
        pools.clear();
        assertThat(dataSource.isClosed()).isTrue();

        // The pool should create at least one physical connection and close it when being cleared.
        assertThat(TestHikariDriver.connectCalls.get()).isGreaterThanOrEqualTo(1);
        assertThat(TestHikariDriver.closedConnections.get()).isGreaterThanOrEqualTo(1);
    }

    private static GaussDBSourceConfig sourceConfigWithDriver(String driverClassName) {
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty(RelationalDatabaseConnectorConfig.SERVER_NAME.name(), "test");

        return new GaussDBSourceConfig(
                org.apache.flink.cdc.connectors.base.options.StartupOptions.initial(),
                Collections.singletonList("test_db"),
                Collections.singletonList("public"),
                null,
                1,
                1,
                1.0d,
                0.0d,
                false,
                false,
                dbzProperties,
                io.debezium.config.Configuration.from(dbzProperties),
                driverClassName,
                "localhost",
                8000,
                "user",
                "password",
                1,
                "UTC",
                Duration.ofSeconds(30),
                1,
                2,
                null,
                false,
                false,
                false,
                "test_slot",
                "mppdb_decoding");
    }

    private static Connection connectionProxy(List<String> executedSql) {
        AtomicInteger closed = new AtomicInteger();
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("createStatement".equals(name)) {
                        return statementProxy(executedSql);
                    }
                    if ("prepareStatement".equals(name) && args != null && args.length > 0) {
                        executedSql.add(String.valueOf(args[0]));
                        return statementProxy(executedSql);
                    }
                    if ("isValid".equals(name)) {
                        return true;
                    }
                    if ("close".equals(name)) {
                        closed.incrementAndGet();
                        return null;
                    }
                    if ("isClosed".equals(name)) {
                        return closed.get() > 0;
                    }
                    if ("getAutoCommit".equals(name)) {
                        return true;
                    }
                    if ("setAutoCommit".equals(name)
                            || "commit".equals(name)
                            || "rollback".equals(name)
                            || "setReadOnly".equals(name)
                            || "setTransactionIsolation".equals(name)
                            || "clearWarnings".equals(name)
                            || "setSchema".equals(name)) {
                        return null;
                    }
                    return defaultValue(method);
                };

        return (Connection)
                Proxy.newProxyInstance(
                        GaussDBConnectionTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        handler);
    }

    private static Statement statementProxy(List<String> executedSql) {
        AtomicInteger closed = new AtomicInteger();
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("execute".equals(name) && args != null && args.length > 0) {
                        executedSql.add(String.valueOf(args[0]));
                        return true;
                    }
                    if ("executeQuery".equals(name) && args != null && args.length > 0) {
                        executedSql.add(String.valueOf(args[0]));
                        return resultSetProxy();
                    }
                    if ("close".equals(name)) {
                        closed.incrementAndGet();
                        return null;
                    }
                    if ("isClosed".equals(name)) {
                        return closed.get() > 0;
                    }
                    return defaultValue(method);
                };

        return (Statement)
                Proxy.newProxyInstance(
                        GaussDBConnectionTest.class.getClassLoader(),
                        new Class<?>[] {Statement.class},
                        handler);
    }

    private static ResultSet resultSetProxy() {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if ("next".equals(method.getName())) {
                        return false;
                    }
                    return defaultValue(method);
                };
        return (ResultSet)
                Proxy.newProxyInstance(
                        GaussDBConnectionTest.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        handler);
    }

    private static Object defaultValue(Method method) {
        Class<?> returnType = method.getReturnType();
        if (returnType.equals(Void.TYPE)) {
            return null;
        }
        if (returnType.equals(Boolean.TYPE)) {
            return false;
        }
        if (returnType.equals(Integer.TYPE)) {
            return 0;
        }
        if (returnType.equals(Long.TYPE)) {
            return 0L;
        }
        if (returnType.equals(Double.TYPE)) {
            return 0.0d;
        }
        if (returnType.equals(Float.TYPE)) {
            return 0.0f;
        }
        if (returnType.equals(Short.TYPE)) {
            return (short) 0;
        }
        if (returnType.equals(Byte.TYPE)) {
            return (byte) 0;
        }
        return null;
    }

    /** A Driver used by Hikari tests that counts connections and close calls. */
    public static final class TestHikariDriver implements Driver {
        static final AtomicInteger connectCalls = new AtomicInteger();
        static final AtomicInteger closedConnections = new AtomicInteger();

        static void reset() {
            connectCalls.set(0);
            closedConnections.set(0);
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            connectCalls.incrementAndGet();
            if (!acceptsURL(url)) {
                return null;
            }
            List<String> executedSql = Collections.synchronizedList(new ArrayList<>());
            return connectionProxyWithCloseCounter(executedSql, closedConnections);
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:gaussdb:");
        }

        @Override
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new java.sql.DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public java.util.logging.Logger getParentLogger()
                throws java.sql.SQLFeatureNotSupportedException {
            throw new java.sql.SQLFeatureNotSupportedException();
        }
    }

    private static Connection connectionProxyWithCloseCounter(
            List<String> executedSql, AtomicInteger closeCounter) {
        AtomicInteger closed = new AtomicInteger();
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("createStatement".equals(name)) {
                        return statementProxy(executedSql);
                    }
                    if ("isValid".equals(name)) {
                        return true;
                    }
                    if ("close".equals(name)) {
                        if (closed.incrementAndGet() == 1) {
                            closeCounter.incrementAndGet();
                        }
                        return null;
                    }
                    if ("isClosed".equals(name)) {
                        return closed.get() > 0;
                    }
                    if ("getAutoCommit".equals(name)) {
                        return true;
                    }
                    if ("setAutoCommit".equals(name)
                            || "commit".equals(name)
                            || "rollback".equals(name)
                            || "setReadOnly".equals(name)
                            || "setTransactionIsolation".equals(name)
                            || "clearWarnings".equals(name)
                            || "setSchema".equals(name)) {
                        return null;
                    }
                    return defaultValue(method);
                };

        return (Connection)
                Proxy.newProxyInstance(
                        GaussDBConnectionTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        handler);
    }
}
