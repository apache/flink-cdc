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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.sink.schema.AddColumnPosition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Unit tests for {@link DorisSchemaChangeManager}. */
public class DorisSchemaChangeManagerTest {

    @Test
    public void testExecuteRetriesDorisSchemaChangeStateAndEventuallySucceeds() throws Exception {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(schemaChangeStateException(), 2);

        Assertions.assertThat(manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isTrue();

        Assertions.assertThat(manager.executeInvocations).isEqualTo(3);
        Assertions.assertThat(manager.sleepInvocations).isEqualTo(2);
    }

    @Test
    public void testExecuteRetriesTopLevelDetailMessageSchemaChangeState() throws Exception {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"Error\","
                                        + "\"code\":2,\"detailMessage\":\"errCode = 1105, "
                                        + "detailMessage = Table[student]'s state(SCHEMA_CHANGE) "
                                        + "is not NORMAL. Do not allow doing ALTER ops\","
                                        + "\"count\":0}"),
                        1);

        Assertions.assertThat(manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isTrue();

        Assertions.assertThat(manager.executeInvocations).isEqualTo(2);
        Assertions.assertThat(manager.sleepInvocations).isOne();
    }

    @Test
    public void testExecuteDoesNotRetrySuccessCodeEvenIfMessageContainsSchemaChangeState() {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"success\","
                                        + "\"code\":0,\"data\":\"Table[student]'s "
                                        + "state(SCHEMA_CHANGE) is not NORMAL. "
                                        + "Do not allow doing ALTER ops\"}"),
                        1);

        Assertions.assertThatThrownBy(
                        () -> manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("code\":0");
        Assertions.assertThat(manager.executeInvocations).isEqualTo(1);
        Assertions.assertThat(manager.sleepInvocations).isZero();
    }

    @Test
    public void testExecuteDoesNotRetryHttpTransportFailure() {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, status: 503, reason: Table[student]'s "
                                        + "state(SCHEMA_CHANGE) is not NORMAL. "
                                        + "Do not allow doing ALTER ops"),
                        1);

        Assertions.assertThatThrownBy(
                        () -> manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("status: 503");
        Assertions.assertThat(manager.executeInvocations).isEqualTo(1);
        Assertions.assertThat(manager.sleepInvocations).isZero();
    }

    @Test
    public void testExecuteDoesNotRetryNonGenericResponseCode() {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"Internal Error\","
                                        + "\"code\":500,\"data\":\"Table[student]'s "
                                        + "state(SCHEMA_CHANGE) is not NORMAL. "
                                        + "Do not allow doing ALTER ops\"}"),
                        1);

        Assertions.assertThatThrownBy(
                        () -> manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("code\":500");
        Assertions.assertThat(manager.executeInvocations).isEqualTo(1);
        Assertions.assertThat(manager.sleepInvocations).isZero();
    }

    @Test
    public void testExecuteDoesNotRetryNonSchemaChangeStateFailure() {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to execute sql: detailMessage = Unknown column age"),
                        2);

        Assertions.assertThatThrownBy(
                        () -> manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("Unknown column age");
        Assertions.assertThat(manager.executeInvocations).isEqualTo(1);
        Assertions.assertThat(manager.sleepInvocations).isZero();
    }

    @Test
    public void testExecuteDoesNotRetryCommonErrorWithoutSchemaChangeState() {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"Error\","
                                        + "\"code\":1,\"data\":{\"detailMessage\":"
                                        + "\"errCode = 1105, detailMessage = Unknown column age\"},"
                                        + "\"count\":0}"),
                        1);

        Assertions.assertThatThrownBy(
                        () -> manager.execute("ALTER TABLE db.tbl MODIFY COLUMN age BIGINT", "db"))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("Unknown column age");
        Assertions.assertThat(manager.executeInvocations).isEqualTo(1);
        Assertions.assertThat(manager.sleepInvocations).isZero();
    }

    @Test
    public void testSchemaChangeRetriesDorisSchemaChangeStateAndEventuallySucceeds()
            throws Exception {
        RetryableSchemaChangeManager manager =
                new RetryableSchemaChangeManager(schemaChangeStateException(), 2);

        Assertions.assertThat(
                        manager.schemaChange(
                                "db",
                                "tbl",
                                Collections.singletonMap("columnName", "age"),
                                "ALTER TABLE db.tbl MODIFY COLUMN age BIGINT"))
                .isTrue();

        Assertions.assertThat(manager.schemaChangeInvocations).isEqualTo(3);
        Assertions.assertThat(manager.sleepInvocations).isEqualTo(2);
    }

    @Test
    public void testAddColumnFallsBackToLastPositionForDorisColumnPositionError() throws Exception {
        PositionFallbackSchemaChangeManager manager =
                new PositionFallbackSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"Error\","
                                        + "\"code\":1,\"data\":\"Failed to execute sql: "
                                        + "java.sql.SQLException: (conn=31) errCode = 1105, "
                                        + "detailMessage = can't add value column age "
                                        + "before key column name\",\"count\":0}"));

        Assertions.assertThat(
                        manager.addColumn(
                                "db",
                                "tbl",
                                new FieldSchema("age", "BIGINT", null, ""),
                                AddColumnPosition.after("id")))
                .isTrue();

        Assertions.assertThat(manager.positions)
                .extracting(AddColumnPosition::getPositionType)
                .containsExactly(
                        AddColumnPosition.PositionType.AFTER, AddColumnPosition.PositionType.LAST);
    }

    @Test
    public void testAddColumnDoesNotFallbackForDorisSuccessCode() {
        PositionFallbackSchemaChangeManager manager =
                new PositionFallbackSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"success\","
                                        + "\"code\":0,\"data\":\"can't add value column age "
                                        + "before key column name\"}"));

        Assertions.assertThatThrownBy(
                        () ->
                                manager.addColumn(
                                        "db",
                                        "tbl",
                                        new FieldSchema("age", "BIGINT", null, ""),
                                        AddColumnPosition.after("id")))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("code\":0");
        Assertions.assertThat(manager.positions)
                .extracting(AddColumnPosition::getPositionType)
                .containsExactly(AddColumnPosition.PositionType.AFTER);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("columnPositionErrorWordings")
    public void testAddColumnFallsBackToLastForEveryColumnPositionWording(String wording)
            throws Exception {
        // Exercises every column-position error wording the classifier must recognize so addColumn
        // falls back to LAST instead of hard-failing the ADD COLUMN: the precise Doris patterns
        // plus the legacy coarse substrings (which guard against Doris versions whose wording the
        // precise patterns do not match).
        PositionFallbackSchemaChangeManager manager =
                new PositionFallbackSchemaChangeManager(dorisPositionExceptionWithDetail(wording));

        Assertions.assertThat(
                        manager.addColumn(
                                "db",
                                "tbl",
                                new FieldSchema("c", "BIGINT", null, ""),
                                AddColumnPosition.after("id")))
                .isTrue();

        Assertions.assertThat(manager.positions)
                .extracting(AddColumnPosition::getPositionType)
                .containsExactly(
                        AddColumnPosition.PositionType.AFTER, AddColumnPosition.PositionType.LAST);
    }

    private static DorisSchemaChangeException dorisPositionExceptionWithDetail(String detail) {
        return new DorisSchemaChangeException(
                "Failed to schemaChange, response: {\"msg\":\"Error\","
                        + "\"code\":1,\"data\":\"Failed to execute sql: "
                        + "java.sql.SQLException: errCode = 1105, detailMessage = "
                        + detail
                        + "\",\"count\":0}");
    }

    private static Stream<String> columnPositionErrorWordings() {
        // Precise Doris column-position patterns.
        // Legacy coarse substrings (no key/value-column prefix) — preserved coverage for Doris
        // versions whose wording the precise patterns do not match.
        return Stream.of(
                "can't add value column c1 as first column",
                "can't add value column c1 before key column id",
                "can't add key column c1 after value column data",
                "cannot add key column c1 after value column data",
                "can not modify column position after itself",
                "can not modify key column c1 after value column data",
                "modify position is invalid",
                "cannot add column c1 as first column",
                "cannot place column c1 before key column",
                "add column c1 after value column",
                "modify column position after itself");
    }

    @Test
    public void testAddColumnDoesNotFallbackForUnrelatedDorisFailure() {
        PositionFallbackSchemaChangeManager manager =
                new PositionFallbackSchemaChangeManager(
                        new DorisSchemaChangeException(
                                "Failed to schemaChange, response: {\"msg\":\"Error\","
                                        + "\"code\":1,\"data\":\"Failed to execute sql: "
                                        + "java.sql.SQLException: errCode = 1105, "
                                        + "detailMessage = Unknown column id\",\"count\":0}"));

        Assertions.assertThatThrownBy(
                        () ->
                                manager.addColumn(
                                        "db",
                                        "tbl",
                                        new FieldSchema("age", "BIGINT", null, ""),
                                        AddColumnPosition.after("id")))
                .isInstanceOf(DorisSchemaChangeException.class)
                .hasMessageContaining("Unknown column id");
        Assertions.assertThat(manager.positions)
                .extracting(AddColumnPosition::getPositionType)
                .containsExactly(AddColumnPosition.PositionType.AFTER);
    }

    private static DorisSchemaChangeException schemaChangeStateException() {
        return new DorisSchemaChangeException(
                "Failed to schemaChange, response: {\"msg\":\"Error\",\"code\":1,"
                        + "\"data\":\"Failed to execute sql: java.sql.SQLException: "
                        + "(conn=31) errCode = 2, detailMessage = Table[student]'s "
                        + "state(SCHEMA_CHANGE) is not NORMAL. Do not allow doing ALTER ops\","
                        + "\"count\":0}");
    }

    private static DorisOptions createDorisOptions() {
        return DorisOptions.builder()
                .setFenodes("127.0.0.1:8030")
                .setUsername("root")
                .setPassword("")
                .build();
    }

    private static class RetryableSchemaChangeManager extends DorisSchemaChangeManager {
        private final DorisSchemaChangeException exception;
        private final int failuresBeforeSuccess;
        private int executeInvocations;
        private int schemaChangeInvocations;
        private int sleepInvocations;

        private RetryableSchemaChangeManager(
                DorisSchemaChangeException exception, int failuresBeforeSuccess) {
            super(createDorisOptions(), null);
            this.exception = exception;
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        protected boolean executeOnce(String ddl, String databaseName)
                throws IOException, org.apache.doris.flink.exception.IllegalArgumentException {
            executeInvocations++;
            if (executeInvocations <= failuresBeforeSuccess) {
                throw exception;
            }
            return true;
        }

        @Override
        protected boolean schemaChangeOnce(
                String databaseName, String tableName, Map<String, Object> params, String ddl)
                throws IOException, org.apache.doris.flink.exception.IllegalArgumentException {
            schemaChangeInvocations++;
            if (schemaChangeInvocations <= failuresBeforeSuccess) {
                throw exception;
            }
            return true;
        }

        @Override
        protected void sleepBeforeRetry() {
            sleepInvocations++;
        }
    }

    private static class PositionFallbackSchemaChangeManager extends DorisSchemaChangeManager {
        private final DorisSchemaChangeException exception;
        private final List<AddColumnPosition> positions = new ArrayList<>();

        private PositionFallbackSchemaChangeManager(DorisSchemaChangeException exception) {
            super(createDorisOptions(), null);
            this.exception = exception;
        }

        @Override
        protected boolean addColumnOnce(
                String databaseName,
                String tableName,
                FieldSchema addFieldSchema,
                AddColumnPosition position)
                throws IOException, org.apache.doris.flink.exception.IllegalArgumentException {
            positions.add(position);
            if (positions.size() == 1) {
                throw exception;
            }
            return true;
        }
    }
}
