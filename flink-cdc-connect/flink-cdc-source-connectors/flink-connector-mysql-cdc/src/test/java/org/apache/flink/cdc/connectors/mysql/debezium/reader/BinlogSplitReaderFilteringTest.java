/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParserListener.TableEvent;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for record filtering with aggregated source table identifiers. */
class BinlogSplitReaderFilteringTest {

    private static final String DATABASE = "cdc_filter_repro";
    private static final String EXACT = "cdc_filter_repro\\.keep_multi";
    private static final String WILDCARD = "cdc_filter_repro\\..*";
    private static final String EXCLUDED =
            "cdc_filter_repro.excluded_single,cdc_filter_repro.excluded_multi";

    @Test
    void testMixedDropWithExactInclude() throws Exception {
        assertSchema(
                "DROP TABLE keep_multi, excluded_multi",
                "keep_multi,excluded_multi",
                EXACT,
                null,
                true);
    }

    @Test
    void testMixedDropWithExactIncludeInReverseOrder() throws Exception {
        assertSchema(
                "DROP TABLE excluded_multi, keep_multi",
                "excluded_multi,keep_multi",
                EXACT,
                null,
                true);
    }

    @Test
    void testMixedDropIfExistsWithExactInclude() throws Exception {
        assertSchema(
                "DROP TABLE IF EXISTS keep_multi, excluded_multi",
                "keep_multi,excluded_multi",
                EXACT,
                null,
                true);
    }

    @Test
    void testAllSubscribedTables() throws Exception {
        assertSchema(
                "DROP TABLE keep_multi, keep_other",
                "keep_multi,keep_other",
                "cdc_filter_repro\\.keep_.*",
                null,
                true);
    }

    @Test
    void testNoSubscribedTable() throws Exception {
        assertSchema(
                "DROP TABLE excluded_single, excluded_multi",
                "excluded_single,excluded_multi",
                EXACT,
                null,
                false);
    }

    @Test
    void testAllExcludedTablesWithWildcardInclude() throws Exception {
        assertSchema(
                "DROP TABLE excluded_single, excluded_multi",
                "excluded_single,excluded_multi",
                WILDCARD,
                EXCLUDED,
                false);
    }

    @Test
    void testSingleSubscribedTable() throws Exception {
        assertSchema("DROP TABLE keep_multi", "keep_multi", EXACT, null, true);
    }

    @Test
    void testSingleExcludedTable() throws Exception {
        assertSchema("DROP TABLE excluded_single", "excluded_single", WILDCARD, EXCLUDED, false);
    }

    @Test
    void testCommaInsideQuotedIdentifierIsNotASecondTable() throws Exception {
        assertSchema("DROP TABLE `keep_multi,ignored`", "keep_multi,ignored", EXACT, null, false);
    }

    @Test
    void testCommaInsideQuotedIdentifierWithRealSubscribedTable() throws Exception {
        assertSchema(
                "DROP TABLE `keep_multi,ignored`, keep_multi",
                "keep_multi,ignored,keep_multi",
                EXACT,
                null,
                true);
    }

    @Test
    void testQuotedCommaTableWithWildcardInclude() throws Exception {
        assertSchema("DROP TABLE `keep,comma`", "keep,comma", WILDCARD, null, true);
    }

    @Test
    void testQualifiedTableOutsideIncludedDatabase() throws Exception {
        assertSchema(
                "DROP TABLE other_database.keep_multi, cdc_filter_repro.excluded_multi",
                "keep_multi,excluded_multi",
                EXACT,
                null,
                false);
    }

    @Test
    void testMixedRenameWithExactInclude() throws Exception {
        assertSchema(
                "RENAME TABLE excluded_multi TO keep_multi, excluded_single TO keep_other",
                "excluded_multi,keep_multi,excluded_single,keep_other",
                EXACT,
                null,
                true);
    }

    @Test
    void testDatabaseDdlHasNoTableToCapture() throws Exception {
        assertSchema("CREATE DATABASE other_database", "", WILDCARD, null, false);
    }

    @Test
    void testDmlInPureBinlogPhaseIsRetained() throws Exception {
        MySqlSourceConfig config = config(EXACT, null);
        BinlogSplitReader reader = reader(config);
        try {
            Field field = BinlogSplitReader.class.getDeclaredField("pureBinlogPhaseTables");
            field.setAccessible(true);
            ((Set<TableId>) field.get(reader)).add(new TableId(DATABASE, null, "keep_multi"));
            assertThat(shouldEmit(reader, dataRecord("keep_multi"))).isTrue();
        } finally {
            reader.close();
        }
    }

    @Test
    void testDmlOutsideMonitoredSplitsIsNotRetained() throws Exception {
        BinlogSplitReader reader = reader(config(EXACT, null));
        try {
            assertThat(shouldEmit(reader, dataRecord("excluded_multi"))).isFalse();
        } finally {
            reader.close();
        }
    }

    @Test
    void testQuotedAlterRetainsCapturedSourceInfo() throws Exception {
        assertSchema(
                "ALTER TABLE `keep,comma` ADD COLUMN amount INT",
                "keep,comma",
                WILDCARD,
                null,
                true);
    }

    @Test
    void testQuotedAlterDoesNotCaptureSubstring() throws Exception {
        assertSchema(
                "ALTER TABLE `keep_multi,ignored` ADD COLUMN amount INT",
                "keep_multi,ignored",
                EXACT,
                null,
                false);
    }

    @Test
    void testQuotedRenameRetainsCapturedSourceInfo() throws Exception {
        assertSchema(
                "RENAME TABLE `keep,comma` TO `keep,renamed`", "keep,comma", WILDCARD, null, true);
    }

    @Test
    void testQuotedRenameDoesNotCaptureSubstring() throws Exception {
        assertSchema(
                "RENAME TABLE `keep_multi,ignored` TO `other,comma`",
                "keep_multi,ignored",
                EXACT,
                null,
                false);
    }

    @Test
    void testNoTableEventsRetainsCapturedSourceInfo() throws Exception {
        assertNoTableEvents("SET NAMES utf8mb4");
        assertSchema("SET NAMES utf8mb4", "keep,comma", WILDCARD, null, true);
    }

    @Test
    void testNoTableEventsRetainsUncapturedSourceInfo() throws Exception {
        assertNoTableEvents("SET NAMES utf8mb4");
        assertSchema("SET NAMES utf8mb4", "keep_multi,ignored", EXACT, null, false);
    }

    @Test
    void testParserFailureRetainsCapturedSourceInfo() throws Exception {
        assertParserFailure("ALTER TABLE `keep,comma` ADD");
        assertSchema("ALTER TABLE `keep,comma` ADD", "keep,comma", WILDCARD, null, true);
    }

    @Test
    void testParserFailureRetainsUncapturedSourceInfo() throws Exception {
        assertParserFailure("ALTER TABLE `keep_multi,ignored` ADD");
        assertSchema(
                "ALTER TABLE `keep_multi,ignored` ADD", "keep_multi,ignored", EXACT, null, false);
    }

    @Test
    void testMalformedHistoryIsNotSilentlyFiltered() throws Exception {
        BinlogSplitReader reader = reader(config(WILDCARD, null));
        try {
            SourceRecord record = schemaRecord("DROP TABLE `keep,comma`", "keep,comma");
            ((Struct) record.value()).put(HISTORY_RECORD_FIELD, "{invalid-json");
            assertThatThrownBy(() -> shouldEmit(reader, record))
                    .isInstanceOf(InvocationTargetException.class)
                    .cause()
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasCauseInstanceOf(IOException.class);
        } finally {
            reader.close();
        }
    }

    private void assertParserFailure(String ddl) {
        MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
        parser.setCurrentDatabase(DATABASE);
        assertThatThrownBy(() -> parser.parse(ddl, new Tables()))
                .isInstanceOfAny(ParsingException.class, MultipleParsingExceptions.class);
    }

    private void assertNoTableEvents(String ddl) {
        MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
        parser.setCurrentDatabase(DATABASE);
        parser.parse(ddl, new Tables());
        List<TableEvent> tableEvents = new ArrayList<>();
        parser.getDdlChanges()
                .getEventsByDatabase(
                        (database, events) ->
                                events.stream()
                                        .filter(event -> event instanceof TableEvent)
                                        .map(event -> (TableEvent) event)
                                        .forEach(tableEvents::add));
        assertThat(tableEvents).isEmpty();
    }

    private void assertSchema(
            String ddl, String sourceTable, String include, String exclude, boolean expected)
            throws Exception {
        BinlogSplitReader reader = reader(config(include, exclude));
        try {
            assertThat(shouldEmit(reader, schemaRecord(ddl, sourceTable))).isEqualTo(expected);
        } finally {
            reader.close();
        }
    }

    private BinlogSplitReader reader(MySqlSourceConfig config) throws Exception {
        // Construction does not connect; split submission and binlog worker startup are
        // unnecessary.
        BinlogSplitReader reader = new BinlogSplitReader(config, 0);
        setField(reader, "capturedTableFilter", config.getTableFilter());
        setField(reader, "maxSplitHighWatermarkMap", new HashMap<>());
        setField(reader, "finishedSplitsInfo", new HashMap<>());
        return reader;
    }

    private void setField(BinlogSplitReader reader, String name, Object value) throws Exception {
        Field field = BinlogSplitReader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(reader, value);
    }

    private boolean shouldEmit(BinlogSplitReader reader, SourceRecord record) throws Exception {
        Method method = BinlogSplitReader.class.getDeclaredMethod("shouldEmit", SourceRecord.class);
        method.setAccessible(true);
        return (boolean) method.invoke(reader, record);
    }

    private MySqlSourceConfig config(String include, String exclude) {
        MySqlSourceConfigFactory factory =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .username("test-user")
                        .password("<REDACTED>")
                        .serverTimeZone("UTC")
                        .databaseList(DATABASE)
                        .tableList(include)
                        .includeSchemaChanges(true);
        if (exclude != null) {
            factory.excludeTableList(exclude);
        }
        return factory.createConfig(0);
    }

    private SourceRecord schemaRecord(String ddl, String sourceTable) throws Exception {
        Schema source = sourceSchema();
        Schema key =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.SchemaChangeKey")
                        .field("databaseName", Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.SchemaChange")
                        .field("source", source)
                        .field(HISTORY_RECORD_FIELD, Schema.STRING_SCHEMA)
                        .build();
        BinlogOffset offset = BinlogOffset.ofBinlogFilePosition("binlog.000374", 751768L);
        HistoryRecord history =
                new HistoryRecord(
                        Collections.emptyMap(),
                        offset.getOffset(),
                        DATABASE,
                        null,
                        ddl,
                        new TableChanges());
        Struct value =
                new Struct(valueSchema)
                        .put(
                                "source",
                                new Struct(source).put("db", DATABASE).put("table", sourceTable))
                        .put(
                                HISTORY_RECORD_FIELD,
                                DocumentWriter.defaultWriter().write(history.document()));
        return new SourceRecord(
                Collections.emptyMap(),
                offset.getOffset(),
                "mysql_binlog_source",
                key,
                new Struct(key).put("databaseName", DATABASE),
                valueSchema,
                value);
    }

    private SourceRecord dataRecord(String table) {
        Schema source = sourceSchema();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.Envelope")
                        .field("source", source)
                        .field("op", Schema.STRING_SCHEMA)
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put("op", "c")
                        .put("source", new Struct(source).put("db", DATABASE).put("table", table));
        return new SourceRecord(
                Collections.emptyMap(),
                BinlogOffset.ofBinlogFilePosition("binlog.000374", 751768L).getOffset(),
                "mysql_binlog_source",
                null,
                null,
                valueSchema,
                value);
    }

    private Schema sourceSchema() {
        return SchemaBuilder.struct()
                .name("test.Source")
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .build();
    }
}
