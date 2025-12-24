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

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class TableDiscoveryUtilsTest {

    private static final String LIST_TABLES_SQL =
            "SELECT n.nspname, c.relname "
                    + "FROM pg_catalog.pg_class c "
                    + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = ? "
                    + "  AND c.relkind IN ('r', 'p')";

    private static final String READ_COLUMNS_SQL =
            "SELECT a.attnum, a.attname, "
                    + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_expr, "
                    + "NOT a.attnotnull AS is_nullable, "
                    + "pg_get_expr(ad.adbin, ad.adrelid) AS column_default "
                    + "FROM pg_catalog.pg_attribute a "
                    + "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
                    + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                    + "LEFT JOIN pg_catalog.pg_attrdef ad "
                    + "  ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum "
                    + "WHERE n.nspname = ? AND c.relname = ? "
                    + "  AND a.attnum > 0 AND NOT a.attisdropped "
                    + "ORDER BY a.attnum";

    private static final String READ_CONSTRAINTS_SQL =
            "SELECT con.contype, con.conkey "
                    + "FROM pg_catalog.pg_constraint con "
                    + "JOIN pg_catalog.pg_class c ON con.conrelid = c.oid "
                    + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                    + "WHERE n.nspname = ? AND c.relname = ? AND con.contype IN ('p', 'u')";

    @Test
    void discoversTablesAndColumnsFromPgCatalog() throws SQLException {
        FakeJdbcConnection jdbc = new FakeJdbcConnection();
        registerPublicSchema(jdbc);

        List<TableDiscoveryUtils.DiscoveredTable> discovered =
                TableDiscoveryUtils.discoverTables(
                        "db", jdbc, includeAllTableFilters(), Collections.singletonList("public"));

        assertThat(discovered).hasSize(1);
        TableDiscoveryUtils.DiscoveredTable table = discovered.get(0);
        assertThat(table.getTableId()).isEqualTo(new TableId("db", "public", "t_with_pk"));

        assertThat(table.getColumns()).hasSize(2);
        assertThat(table.getColumns().get(0).getName()).isEqualTo("id");
        assertThat(table.getColumns().get(0).getTypeExpression()).isEqualTo("integer");
        assertThat(table.getColumns().get(0).isNullable()).isFalse();
        assertThat(table.getColumns().get(0).getDefaultExpression()).isNull();
        assertThat(table.getColumns().get(1).getName()).isEqualTo("name");
        assertThat(table.getColumns().get(1).isNullable()).isTrue();
        assertThat(table.getColumns().get(1).getDefaultExpression()).isEqualTo("'unknown'::text");
    }

    @Test
    void identifiesPrimaryAndUniqueKeys() throws SQLException {
        FakeJdbcConnection jdbc = new FakeJdbcConnection();
        registerPublicSchema(jdbc);

        List<TableDiscoveryUtils.DiscoveredTable> discovered =
                TableDiscoveryUtils.discoverTables(
                        "db", jdbc, includeAllTableFilters(), Collections.singletonList("public"));

        TableDiscoveryUtils.DiscoveredTable table = discovered.get(0);
        assertThat(table.getPrimaryKeyColumnNames()).containsExactly("id");
        assertThat(table.getUniqueKeyColumnNames())
                .containsExactly(Collections.singletonList("name"));
    }

    @Test
    void skipsTablesWithoutPrimaryKeyAndLogsWarn() throws SQLException {
        FakeJdbcConnection jdbc = new FakeJdbcConnection();
        registerPublicSchema(jdbc);

        CapturingAppender appender =
                CapturingAppender.install(TableDiscoveryUtils.class.getName(), Level.WARN);
        try {
            List<TableId> tables =
                    TableDiscoveryUtils.listTables(
                            "db",
                            jdbc,
                            includeAllTableFilters(),
                            Collections.singletonList("public"));
            assertThat(tables).containsExactly(new TableId("db", "public", "t_with_pk"));

            assertThat(appender.events())
                    .anySatisfy(
                            event ->
                                    assertThat(event.getMessage().getFormattedMessage())
                                            .contains("Skipping table")
                                            .contains("t_without_pk"));
        } finally {
            appender.remove();
        }
    }

    private static void registerPublicSchema(FakeJdbcConnection jdbc) {
        jdbc.whenQuery(LIST_TABLES_SQL, "public")
                .thenReturn(row("public", "t_with_pk"), row("public", "t_without_pk"));

        jdbc.whenQuery(READ_COLUMNS_SQL, "public", "t_with_pk")
                .thenReturn(
                        row(1, "id", "integer", false, null),
                        row(2, "name", "text", true, "'unknown'::text"));
        jdbc.whenQuery(READ_CONSTRAINTS_SQL, "public", "t_with_pk")
                .thenReturn(row("p", new Object[] {1}), row("u", new Object[] {2}));

        jdbc.whenQuery(READ_COLUMNS_SQL, "public", "t_without_pk")
                .thenReturn(row(1, "id", "integer", false, null));
        jdbc.whenQuery(READ_CONSTRAINTS_SQL, "public", "t_without_pk").thenReturn();
    }

    private static RelationalTableFilters includeAllTableFilters() {
        return new RelationalTableFilters(
                Configuration.create().build(), tableId -> true, TableId::toString);
    }

    private static Object[] row(Object... values) {
        return values;
    }

    private static class FakeJdbcConnection extends JdbcConnection {
        private final Map<QueryKey, List<Object[]>> results = new HashMap<>();

        FakeJdbcConnection() {
            super(
                    JdbcConfiguration.create().build(),
                    config -> {
                        throw new UnsupportedOperationException("No real JDBC connection");
                    },
                    "\"",
                    "\"");
        }

        WhenQuery whenQuery(String sql, Object... params) {
            return new WhenQuery(this, sql, Arrays.asList(params));
        }

        @Override
        public JdbcConnection prepareQuery(
                String sql, StatementPreparer statementPreparer, ResultSetConsumer resultConsumer)
                throws SQLException {
            ParamCapturingPreparedStatement stmt = new ParamCapturingPreparedStatement();
            PreparedStatement proxy =
                    (PreparedStatement)
                            Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class[] {PreparedStatement.class},
                                    stmt);
            statementPreparer.accept(proxy);

            QueryKey key = new QueryKey(sql, stmt.paramsInOrder());
            List<Object[]> rows =
                    results.getOrDefault(
                            key,
                            results.getOrDefault(new QueryKey(sql, Collections.emptyList()), null));
            if (rows == null) {
                throw new AssertionError("No fake result registered for query: " + key);
            }
            resultConsumer.accept(ResultSets.fromRows(rows));
            return this;
        }

        private void register(String sql, List<Object> params, List<Object[]> rows) {
            results.put(new QueryKey(sql, params), rows);
        }

        private static class WhenQuery {
            private final FakeJdbcConnection jdbc;
            private final String sql;
            private final List<Object> params;

            private WhenQuery(FakeJdbcConnection jdbc, String sql, List<Object> params) {
                this.jdbc = jdbc;
                this.sql = sql;
                this.params = params;
            }

            void thenReturn(Object[]... rows) {
                jdbc.register(sql, params, Arrays.asList(rows));
            }
        }
    }

    private static class QueryKey {
        private final String sql;
        private final List<Object> params;

        private QueryKey(String sql, List<Object> params) {
            this.sql = normalizeSql(sql);
            this.params = Collections.unmodifiableList(new ArrayList<>(params));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof QueryKey)) {
                return false;
            }
            QueryKey queryKey = (QueryKey) o;
            return Objects.equals(sql, queryKey.sql) && Objects.equals(params, queryKey.params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sql, params);
        }

        @Override
        public String toString() {
            return "QueryKey{" + "sql='" + sql + '\'' + ", params=" + params + '}';
        }

        private static String normalizeSql(String sql) {
            return sql == null ? "" : sql.replaceAll("\\s+", " ").trim();
        }
    }

    private static class ParamCapturingPreparedStatement
            implements java.lang.reflect.InvocationHandler {
        private final Map<Integer, Object> params = new HashMap<>();

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            String name = method.getName();
            if ("setString".equals(name) || "setObject".equals(name)) {
                params.put((Integer) args[0], args[1]);
                return null;
            }
            if ("setInt".equals(name)) {
                params.put((Integer) args[0], args[1]);
                return null;
            }
            if ("close".equals(name)) {
                return null;
            }

            Class<?> returnType = method.getReturnType();
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            if (returnType == double.class) {
                return 0d;
            }
            if (returnType == float.class) {
                return 0f;
            }
            return null;
        }

        private List<Object> paramsInOrder() {
            if (params.isEmpty()) {
                return Collections.emptyList();
            }
            int max =
                    params.keySet().stream()
                            .filter(Objects::nonNull)
                            .mapToInt(Integer::intValue)
                            .max()
                            .orElse(0);
            List<Object> ordered = new ArrayList<>(max);
            for (int i = 1; i <= max; i++) {
                ordered.add(params.get(i));
            }
            return ordered;
        }
    }

    private static class ResultSets {
        static ResultSet fromRows(List<Object[]> rows) {
            return (ResultSet)
                    Proxy.newProxyInstance(
                            ResultSets.class.getClassLoader(),
                            new Class[] {ResultSet.class},
                            new ResultSetHandler(rows));
        }
    }

    private static class ResultSetHandler implements java.lang.reflect.InvocationHandler {
        private final List<Object[]> rows;
        private int index = -1;
        private boolean wasNull;

        private ResultSetHandler(List<Object[]> rows) {
            this.rows = rows == null ? Collections.emptyList() : rows;
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args)
                throws Throwable {
            String name = method.getName();
            if ("next".equals(name)) {
                index++;
                return index < rows.size();
            }
            if ("wasNull".equals(name)) {
                return wasNull;
            }
            if ("getString".equals(name)
                    && args != null
                    && args.length == 1
                    && args[0] instanceof Integer) {
                Object value = valueAt((Integer) args[0]);
                wasNull = value == null;
                return value == null ? null : value.toString();
            }
            if ("getInt".equals(name)
                    && args != null
                    && args.length == 1
                    && args[0] instanceof Integer) {
                Object value = valueAt((Integer) args[0]);
                wasNull = value == null;
                if (value == null) {
                    return 0;
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseInt(value.toString());
            }
            if ("getBoolean".equals(name)
                    && args != null
                    && args.length == 1
                    && args[0] instanceof Integer) {
                Object value = valueAt((Integer) args[0]);
                wasNull = value == null;
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
                }
                return Boolean.parseBoolean(value.toString());
            }
            if ("getArray".equals(name)
                    && args != null
                    && args.length == 1
                    && args[0] instanceof Integer) {
                Object value = valueAt((Integer) args[0]);
                wasNull = value == null;
                if (value == null) {
                    return null;
                }
                if (value instanceof java.sql.Array) {
                    return value;
                }
                Object array = value instanceof Object[] ? value : new Object[] {value};
                return ArraysProxy.of(array);
            }
            if ("close".equals(name)) {
                return null;
            }

            Class<?> returnType = method.getReturnType();
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            return null;
        }

        private Object valueAt(int columnIndex) {
            if (index < 0 || index >= rows.size()) {
                throw new IllegalStateException("ResultSet cursor not positioned on a row");
            }
            Object[] row = rows.get(index);
            int idx = columnIndex - 1;
            if (idx < 0 || idx >= row.length) {
                return null;
            }
            return row[idx];
        }
    }

    private static class ArraysProxy {
        static java.sql.Array of(Object array) {
            return (java.sql.Array)
                    Proxy.newProxyInstance(
                            ArraysProxy.class.getClassLoader(),
                            new Class[] {java.sql.Array.class},
                            (proxy, method, args) -> {
                                String name = method.getName();
                                if ("getArray".equals(name)) {
                                    return array;
                                }
                                if ("free".equals(name)) {
                                    return null;
                                }
                                if ("getBaseType".equals(name)) {
                                    return java.sql.Types.INTEGER;
                                }
                                if ("getBaseTypeName".equals(name)) {
                                    return "integer";
                                }
                                Class<?> returnType = method.getReturnType();
                                if (returnType == boolean.class) {
                                    return false;
                                }
                                if (returnType == int.class) {
                                    return 0;
                                }
                                if (returnType == long.class) {
                                    return 0L;
                                }
                                return null;
                            });
        }
    }

    private static class CapturingAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();
        private final LoggerContext context;
        private final LoggerConfig loggerConfig;
        private final Level previousLevel;

        private CapturingAppender(
                String name,
                LoggerContext context,
                LoggerConfig loggerConfig,
                Level previousLevel) {
            super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
            this.context = context;
            this.loggerConfig = loggerConfig;
            this.previousLevel = previousLevel;
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        List<LogEvent> events() {
            return events;
        }

        void remove() {
            loggerConfig.removeAppender(getName());
            loggerConfig.setLevel(previousLevel);
            stop();
            context.updateLoggers();
        }

        static CapturingAppender install(String loggerName, Level level) {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
            LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);

            Level previousLevel = loggerConfig.getLevel();
            loggerConfig.setLevel(Level.ALL);

            CapturingAppender appender =
                    new CapturingAppender(
                            "capturingAppender-" + System.nanoTime(),
                            context,
                            loggerConfig,
                            previousLevel);
            appender.start();
            config.addAppender(appender);
            loggerConfig.addAppender(appender, level, null);
            context.updateLoggers();
            return appender;
        }
    }
}
