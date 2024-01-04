package com.ververica.cdc.connectors.mysql.source.utils;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link StatementUtils}.
 */
public class StatementUtilsTest extends MySqlSourceTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
    private static MySqlConnection mySqlConnection;

    @BeforeClass
    public static void init() throws SQLException {
        customerDatabase.createAndInitialize();
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customerDatabase.getUsername());
        properties.put("database.password", customerDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        mySqlConnection = DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (mySqlConnection != null) {
            mySqlConnection.close();
        }
    }

    @Test
    public void testCompareValueByQuery() throws SQLException {
        TableId tableId = new TableId(customerDatabase.getDatabaseName(), null, "unevenly_shopping_cart_utf8_bin");
        int resultBin1 = StatementUtils.compareValueByQuery("aaaa", "zzzz", mySqlConnection, tableId, "pk_varchar");
        int resultBin2 = StatementUtils.compareValueByQuery("zzzz", "ZZZZ", mySqlConnection, tableId, "pk_varchar");
        int resultBin3 = StatementUtils.compareValueByQuery("9", "a", mySqlConnection, tableId, "pk_varchar");
        assertEquals(-1, resultBin1);
        assertEquals(1, resultBin2);
        assertEquals(-1, resultBin3);

        TableId tableId2 = new TableId(customerDatabase.getDatabaseName(), null, "unevenly_shopping_cart_utf8_ci");
        int resultCi1 = StatementUtils.compareValueByQuery("aaaa", "zzzz", mySqlConnection, tableId2, "pk_varchar");
        int resultCi2 = StatementUtils.compareValueByQuery("zzzz", "ZZZZ", mySqlConnection, tableId2, "pk_varchar");
        int resultCi3 = StatementUtils.compareValueByQuery("ZZZZ", "aaaa", mySqlConnection, tableId2, "pk_varchar");
        int resultCi4 = StatementUtils.compareValueByQuery("9", "a", mySqlConnection, tableId2, "pk_varchar");
        assertEquals(-1, resultCi1);
        assertEquals(0, resultCi2);
        assertEquals(1, resultCi3);
        assertEquals(-1, resultCi4);
    }

    public static MySqlSourceConfig getConfig(
            UniqueDatabase database,
            String[] captureTables,
            int splitSize,
            boolean skipSnapshotBackfill) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .databaseList(database.getDatabaseName())
                .tableList(captureTableIds)
                .serverId("1001-1002")
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(database.getUsername())
                .splitSize(splitSize)
                .fetchSize(2)
                .password(database.getPassword())
                .skipSnapshotBackfill(skipSnapshotBackfill)
                .createConfig(0);
    }

}
