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

package org.apache.flink.cdc.connectors.db2;

import io.debezium.connector.db2.Db2ChangeTable;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Lsn;
import io.debezium.jdbc.JdbcConfiguration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** Testcontainers-backed tests for {@link Db2Connection}. */
class Db2ConnectionContainerTest extends Db2TestBase {

    @Test
    void testListOfNewChangeTablesDetectsRecapturedTableAfterSchemaChange() throws Exception {
        initializeDb2Table("inventory", "PRODUCTS");

        try (Db2Connection db2Connection = createDb2Connection();
                Connection jdbcConnection = getJdbcConnection();
                Statement statement = jdbcConnection.createStatement()) {
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS(NAME, DESCRIPTION, WEIGHT) "
                            + "VALUES ('baseline robot', 'before schema change', 1.0)");
            Lsn fromLsn = waitForAvailableMaxLsn(db2Connection);
            db2Connection.rollback();

            statement.execute("ALTER TABLE DB2INST1.PRODUCTS ADD COLUMN VOLUME FLOAT");
            statement.execute("CALL ASNCDC.REMOVETABLE('DB2INST1', 'PRODUCTS')");
            statement.execute("CALL ASNCDC.ADDTABLE('DB2INST1', 'PRODUCTS')");
            statement.execute(
                    "UPDATE ASNCDC.IBMSNAP_REGISTER "
                            + "SET CD_OLD_SYNCHPOINT = X'00000000000000000000000000000000' "
                            + "WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = 'PRODUCTS'");
            statement.execute("VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc')");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS(NAME, DESCRIPTION, WEIGHT, VOLUME) "
                            + "VALUES ('schema robot', 'after schema change', 2.0, 13.5)");

            assertChangeTableContainsColumn(jdbcConnection, "VOLUME");
            Awaitility.await("new DB2 CDC capture table is discoverable")
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Lsn toLsn = db2Connection.getMaxLsn();
                                if (toLsn.compareTo(fromLsn) <= 0) {
                                    db2Connection.rollback();
                                }
                                assertThat(toLsn.compareTo(fromLsn)).isGreaterThan(0);
                                Set<Db2ChangeTable> newChangeTables =
                                        db2Connection.listOfNewChangeTables(fromLsn, toLsn);
                                assertThat(newChangeTables)
                                        .isNotEmpty()
                                        .anySatisfy(
                                                table -> {
                                                    assertThat(table.getStartLsn())
                                                            .isEqualTo(toLsn);
                                                    assertThat(
                                                                    table.getStartLsn()
                                                                            .compareTo(fromLsn))
                                                            .isGreaterThanOrEqualTo(0);
                                                    assertThat(table.getStartLsn().compareTo(toLsn))
                                                            .isLessThanOrEqualTo(0);
                                                });
                            });
        }
    }

    private Db2Connection createDb2Connection() {
        JdbcConfiguration jdbcConfiguration =
                JdbcConfiguration.create()
                        .with(JdbcConfiguration.HOSTNAME, DB2_CONTAINER.getHost())
                        .with(JdbcConfiguration.PORT, DB2_CONTAINER.getMappedPort(DB2_PORT))
                        .with(JdbcConfiguration.DATABASE, DB2_CONTAINER.getDatabaseName())
                        .with(JdbcConfiguration.USER, DB2_CONTAINER.getUsername())
                        .with(JdbcConfiguration.PASSWORD, DB2_CONTAINER.getPassword())
                        .build();
        return new Db2Connection(jdbcConfiguration);
    }

    private Lsn waitForAvailableMaxLsn(Db2Connection db2Connection) {
        final Lsn[] maxLsn = new Lsn[1];
        Awaitility.await("DB2 CDC max LSN")
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            maxLsn[0] = db2Connection.getMaxLsn();
                            assertThat(maxLsn[0].isAvailable()).isTrue();
                        });
        return maxLsn[0];
    }

    private void assertChangeTableContainsColumn(Connection connection, String columnName)
            throws Exception {
        try (ResultSet resultSet =
                connection
                        .createStatement()
                        .executeQuery(
                                "SELECT COUNT(*) FROM SYSCAT.COLUMNS "
                                        + "WHERE TABSCHEMA = 'ASNCDC' "
                                        + "AND TABNAME = 'CDC_DB2INST1_PRODUCTS' "
                                        + "AND COLNAME = '"
                                        + columnName
                                        + "'")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);
        }
    }
}
