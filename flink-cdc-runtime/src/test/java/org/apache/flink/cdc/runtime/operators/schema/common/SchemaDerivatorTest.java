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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Unit test for {@link SchemaDerivator}. */
public class SchemaDerivatorTest extends SchemaTestBase {
    private static final Set<TableId> ALL_UPSTREAM_TABLE_IDS =
            IntStream.rangeClosed(0, 5)
                    .boxed()
                    .flatMap(
                            dbIdx ->
                                    IntStream.rangeClosed(1, 3)
                                            .mapToObj(
                                                    tblIdx ->
                                                            TableId.tableId(
                                                                    "db_" + dbIdx,
                                                                    "table_" + tblIdx)))
                    .collect(Collectors.toSet());

    private static final Table<TableId, Integer, Schema> DUMMY_UPSTREAM_SCHEMA_TABLE =
            HashBasedTable.create();

    static {
        ALL_UPSTREAM_TABLE_IDS.forEach(
                tbl -> {
                    for (int i = 0; i < 2; i++) {
                        DUMMY_UPSTREAM_SCHEMA_TABLE.put(
                                tbl,
                                i,
                                Schema.newBuilder()
                                        .physicalColumn(
                                                "id",
                                                DataTypes.INT(),
                                                String.format("%s @ %s", tbl, i))
                                        .build());
                    }
                });
    }

    private static final TableId NORMALIZE_TEST_TABLE_ID = TableId.parse("foo.bar.baz");
    private static final Schema NORMALIZE_TEST_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.VARCHAR(128))
                    .physicalColumn("age", DataTypes.FLOAT())
                    .physicalColumn("notes", DataTypes.STRING())
                    .build();
    private static final MetadataApplier MOCKED_METADATA_APPLIER =
            schemaChangeEvent -> {
                throw new UnsupportedOperationException(
                        "Mocked metadata applier doesn't really do anything.");
            };

    private static Set<String> getAffected(String... tables) {
        return SchemaDerivator.getAffectedEvolvedTables(
                        TABLE_ID_ROUTER,
                        Arrays.stream(tables).map(TableId::parse).collect(Collectors.toSet()))
                .stream()
                .map(TableId::toString)
                .collect(Collectors.toSet());
    }

    private static Set<String> reverseLookupTable(String tableId) {
        return SchemaDerivator.reverseLookupDependingUpstreamTables(
                        TABLE_ID_ROUTER, TableId.parse(tableId), DUMMY_UPSTREAM_SCHEMA_TABLE)
                .stream()
                .map(TableId::toString)
                .collect(Collectors.toSet());
    }

    private static Set<String> reverseLookupSchema(String tableId) {
        return SchemaDerivator.reverseLookupDependingUpstreamSchemas(
                        TABLE_ID_ROUTER, TableId.parse(tableId), DUMMY_UPSTREAM_SCHEMA_TABLE)
                .stream()
                .map(schema -> schema.getColumn("id").get().getComment())
                .collect(Collectors.toSet());
    }

    private static List<SchemaChangeEvent> normalizeEvent(
            SchemaChangeBehavior behavior, SchemaChangeEvent... events) {
        return SchemaDerivator.normalizeSchemaChangeEvents(
                NORMALIZE_TEST_SCHEMA,
                Arrays.stream(events).collect(Collectors.toList()),
                behavior,
                MOCKED_METADATA_APPLIER);
    }

    @Test
    void testGetAffectedEvolvedTables() {
        assertThat(getAffected()).isEmpty();

        // No routing rule, behaves like one-to-one routing
        assertThat(getAffected("db_0.table_1")).containsExactlyInAnyOrder("db_0.table_1");
        assertThat(getAffected("db_0.table_2")).containsExactlyInAnyOrder("db_0.table_2");
        assertThat(getAffected("db_0.table_3")).containsExactlyInAnyOrder("db_0.table_3");
        assertThat(getAffected("db_0.table_1", "db_0.table_2"))
                .containsExactlyInAnyOrder("db_0.table_1", "db_0.table_2");
        assertThat(getAffected("db_0.table_1", "db_0.table_3"))
                .containsExactlyInAnyOrder("db_0.table_1", "db_0.table_3");
        assertThat(getAffected("db_0.table_2", "db_0.table_3"))
                .containsExactlyInAnyOrder("db_0.table_2", "db_0.table_3");
        assertThat(getAffected("db_0.table_1", "db_0.table_2", "db_0.table_3"))
                .containsExactlyInAnyOrder("db_0.table_1", "db_0.table_2", "db_0.table_3");

        // One-to-one routing
        assertThat(getAffected("db_1.table_1")).containsExactlyInAnyOrder("db_1.table_1");
        assertThat(getAffected("db_1.table_2")).containsExactlyInAnyOrder("db_1.table_2");
        assertThat(getAffected("db_1.table_3")).containsExactlyInAnyOrder("db_1.table_3");
        assertThat(getAffected("db_1.table_1", "db_1.table_2"))
                .containsExactlyInAnyOrder("db_1.table_1", "db_1.table_2");
        assertThat(getAffected("db_1.table_1", "db_1.table_3"))
                .containsExactlyInAnyOrder("db_1.table_1", "db_1.table_3");
        assertThat(getAffected("db_1.table_2", "db_1.table_3"))
                .containsExactlyInAnyOrder("db_1.table_2", "db_1.table_3");
        assertThat(getAffected("db_1.table_1", "db_1.table_2", "db_1.table_3"))
                .containsExactlyInAnyOrder("db_1.table_1", "db_1.table_2", "db_1.table_3");

        // One-to-one routing, but twisted
        assertThat(getAffected("db_2.table_1")).containsExactlyInAnyOrder("db_2.table_2");
        assertThat(getAffected("db_2.table_2")).containsExactlyInAnyOrder("db_2.table_3");
        assertThat(getAffected("db_2.table_3")).containsExactlyInAnyOrder("db_2.table_1");
        assertThat(getAffected("db_2.table_1", "db_2.table_2"))
                .containsExactlyInAnyOrder("db_2.table_2", "db_2.table_3");
        assertThat(getAffected("db_2.table_1", "db_2.table_3"))
                .containsExactlyInAnyOrder("db_2.table_2", "db_2.table_1");
        assertThat(getAffected("db_2.table_2", "db_2.table_3"))
                .containsExactlyInAnyOrder("db_2.table_3", "db_2.table_1");
        assertThat(getAffected("db_2.table_1", "db_2.table_2", "db_2.table_3"))
                .containsExactlyInAnyOrder("db_2.table_2", "db_2.table_3", "db_2.table_1");

        // Merging-table routing
        assertThat(getAffected("db_3.table_1")).containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(getAffected("db_3.table_2")).containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(getAffected("db_3.table_3")).containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(getAffected("db_3.table_1", "db_3.table_2"))
                .containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(getAffected("db_3.table_1", "db_3.table_3"))
                .containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(getAffected("db_3.table_2", "db_3.table_3"))
                .containsExactlyInAnyOrder("db_3.table_merged");
        assertThat(getAffected("db_3.table_1", "db_3.table_2", "db_3.table_3"))
                .containsExactlyInAnyOrder("db_3.table_merged");

        // Broadcasting routing
        assertThat(getAffected("db_4.table_1"))
                .containsExactlyInAnyOrder("db_4.table_a", "db_4.table_b", "db_4.table_c");
        assertThat(getAffected("db_4.table_2"))
                .containsExactlyInAnyOrder("db_4.table_b", "db_4.table_c");
        assertThat(getAffected("db_4.table_3")).containsExactlyInAnyOrder("db_4.table_c");
        assertThat(getAffected("db_4.table_1", "db_4.table_2"))
                .containsExactlyInAnyOrder("db_4.table_a", "db_4.table_b", "db_4.table_c");
        assertThat(getAffected("db_4.table_1", "db_4.table_3"))
                .containsExactlyInAnyOrder("db_4.table_a", "db_4.table_b", "db_4.table_c");
        assertThat(getAffected("db_4.table_2", "db_4.table_3"))
                .containsExactlyInAnyOrder("db_4.table_b", "db_4.table_c");
        assertThat(getAffected("db_4.table_1", "db_4.table_2", "db_4.table_3"))
                .containsExactlyInAnyOrder("db_4.table_a", "db_4.table_b", "db_4.table_c");

        // RepSym routing
        assertThat(getAffected("db_5.table_1", "db_5.table_2"))
                .containsExactlyInAnyOrder(
                        "db_5.prefix_table_1_suffix", "db_5.prefix_table_2_suffix");
        assertThat(getAffected("db_5.table_1", "db_5.table_3"))
                .containsExactlyInAnyOrder(
                        "db_5.prefix_table_1_suffix", "db_5.prefix_table_3_suffix");
        assertThat(getAffected("db_5.table_2", "db_5.table_3"))
                .containsExactlyInAnyOrder(
                        "db_5.prefix_table_2_suffix", "db_5.prefix_table_3_suffix");
        assertThat(getAffected("db_5.table_1", "db_5.table_2", "db_5.table_3"))
                .containsExactlyInAnyOrder(
                        "db_5.prefix_table_1_suffix",
                        "db_5.prefix_table_2_suffix",
                        "db_5.prefix_table_3_suffix");
    }

    @Test
    void testReverseLookupDependingUpstreamTables() {
        assertThat(reverseLookupTable("db_0.table_1")).containsExactlyInAnyOrder("db_0.table_1");
        assertThat(reverseLookupTable("db_0.table_2")).containsExactlyInAnyOrder("db_0.table_2");
        assertThat(reverseLookupTable("db_0.table_3")).containsExactlyInAnyOrder("db_0.table_3");

        assertThat(reverseLookupTable("db_1.table_1")).containsExactlyInAnyOrder("db_1.table_1");
        assertThat(reverseLookupTable("db_1.table_2")).containsExactlyInAnyOrder("db_1.table_2");
        assertThat(reverseLookupTable("db_1.table_3")).containsExactlyInAnyOrder("db_1.table_3");

        assertThat(reverseLookupTable("db_2.table_1")).containsExactlyInAnyOrder("db_2.table_3");
        assertThat(reverseLookupTable("db_2.table_2")).containsExactlyInAnyOrder("db_2.table_1");
        assertThat(reverseLookupTable("db_2.table_3")).containsExactlyInAnyOrder("db_2.table_2");

        assertThat(reverseLookupTable("db_3.table_merged"))
                .containsExactlyInAnyOrder("db_3.table_1", "db_3.table_2", "db_3.table_3");

        assertThat(reverseLookupTable("db_4.table_a")).containsExactlyInAnyOrder("db_4.table_1");
        assertThat(reverseLookupTable("db_4.table_b"))
                .containsExactlyInAnyOrder("db_4.table_1", "db_4.table_2");
        assertThat(reverseLookupTable("db_4.table_c"))
                .containsExactlyInAnyOrder("db_4.table_1", "db_4.table_2", "db_4.table_3");

        assertThat(reverseLookupTable("db_5.prefix_table_1_suffix"))
                .containsExactlyInAnyOrder("db_5.table_1");
        assertThat(reverseLookupTable("db_5.prefix_table_2_suffix"))
                .containsExactlyInAnyOrder("db_5.table_2");
        assertThat(reverseLookupTable("db_5.prefix_table_3_suffix"))
                .containsExactlyInAnyOrder("db_5.table_3");
    }

    @Test
    void testReverseLookupDependingUpstreamSchemas() {
        assertThat(reverseLookupSchema("db_0.table_1"))
                .containsExactlyInAnyOrder("db_0.table_1 @ 0", "db_0.table_1 @ 1");
        assertThat(reverseLookupSchema("db_0.table_2"))
                .containsExactlyInAnyOrder("db_0.table_2 @ 0", "db_0.table_2 @ 1");
        assertThat(reverseLookupSchema("db_0.table_3"))
                .containsExactlyInAnyOrder("db_0.table_3 @ 0", "db_0.table_3 @ 1");

        assertThat(reverseLookupSchema("db_1.table_1"))
                .containsExactlyInAnyOrder("db_1.table_1 @ 0", "db_1.table_1 @ 1");
        assertThat(reverseLookupSchema("db_1.table_2"))
                .containsExactlyInAnyOrder("db_1.table_2 @ 0", "db_1.table_2 @ 1");
        assertThat(reverseLookupSchema("db_1.table_3"))
                .containsExactlyInAnyOrder("db_1.table_3 @ 0", "db_1.table_3 @ 1");

        assertThat(reverseLookupSchema("db_2.table_1"))
                .containsExactlyInAnyOrder("db_2.table_3 @ 0", "db_2.table_3 @ 1");
        assertThat(reverseLookupSchema("db_2.table_2"))
                .containsExactlyInAnyOrder("db_2.table_1 @ 0", "db_2.table_1 @ 1");
        assertThat(reverseLookupSchema("db_2.table_3"))
                .containsExactlyInAnyOrder("db_2.table_2 @ 0", "db_2.table_2 @ 1");

        assertThat(reverseLookupSchema("db_3.table_merged"))
                .containsExactlyInAnyOrder(
                        "db_3.table_1 @ 0",
                        "db_3.table_1 @ 1",
                        "db_3.table_2 @ 0",
                        "db_3.table_2 @ 1",
                        "db_3.table_3 @ 0",
                        "db_3.table_3 @ 1");

        assertThat(reverseLookupSchema("db_4.table_a"))
                .containsExactlyInAnyOrder("db_4.table_1 @ 0", "db_4.table_1 @ 1");
        assertThat(reverseLookupSchema("db_4.table_b"))
                .containsExactlyInAnyOrder(
                        "db_4.table_1 @ 0",
                        "db_4.table_1 @ 1",
                        "db_4.table_2 @ 0",
                        "db_4.table_2 @ 1");
        assertThat(reverseLookupSchema("db_4.table_c"))
                .containsExactlyInAnyOrder(
                        "db_4.table_1 @ 0",
                        "db_4.table_1 @ 1",
                        "db_4.table_2 @ 0",
                        "db_4.table_2 @ 1",
                        "db_4.table_3 @ 0",
                        "db_4.table_3 @ 1");

        assertThat(reverseLookupSchema("db_5.prefix_table_1_suffix"))
                .containsExactlyInAnyOrder("db_5.table_1 @ 0", "db_5.table_1 @ 1");
        assertThat(reverseLookupSchema("db_5.prefix_table_2_suffix"))
                .containsExactlyInAnyOrder("db_5.table_2 @ 0", "db_5.table_2 @ 1");
        assertThat(reverseLookupSchema("db_5.prefix_table_3_suffix"))
                .containsExactlyInAnyOrder("db_5.table_3 @ 0", "db_5.table_3 @ 1");
    }

    @Test
    void testNormalizeSchemaChangeEventsInEvolveMode() {

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new CreateTableEvent(
                                        NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA)))
                .containsExactly(
                        new CreateTableEvent(NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new AddColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "added_flag", DataTypes.BOOLEAN()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "id")))))
                .containsExactly(
                        new AddColumnEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "added_flag", DataTypes.BOOLEAN()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "id"))));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new AlterColumnTypeEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", DataTypes.DOUBLE()))))
                .containsExactly(
                        new AlterColumnTypeEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonMap("age", DataTypes.DOUBLE()),
                                Collections.singletonMap("age", DataTypes.FLOAT())));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new RenameColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", "aging"))))
                .containsExactly(
                        new RenameColumnEvent(
                                NORMALIZE_TEST_TABLE_ID, Collections.singletonMap("age", "aging")));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new DropColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList("notes"))))
                .containsExactly(
                        new DropColumnEvent(
                                NORMALIZE_TEST_TABLE_ID, Collections.singletonList("notes")));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .containsExactly(new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.EVOLVE,
                                new DropTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .containsExactly(new DropTableEvent(NORMALIZE_TEST_TABLE_ID));
    }

    @Test
    void testNormalizeSchemaChangeEventsInTryEvolveMode() {

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new CreateTableEvent(
                                        NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA)))
                .containsExactly(
                        new CreateTableEvent(NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new AddColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "added_flag", DataTypes.BOOLEAN()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "id")))))
                .containsExactly(
                        new AddColumnEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "added_flag", DataTypes.BOOLEAN()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "id"))));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new AlterColumnTypeEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", DataTypes.DOUBLE()))))
                .containsExactly(
                        new AlterColumnTypeEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonMap("age", DataTypes.DOUBLE()),
                                Collections.singletonMap("age", DataTypes.FLOAT())));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new RenameColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", "aging"))))
                .containsExactly(
                        new RenameColumnEvent(
                                NORMALIZE_TEST_TABLE_ID, Collections.singletonMap("age", "aging")));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new DropColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList("notes"))))
                .containsExactly(
                        new DropColumnEvent(
                                NORMALIZE_TEST_TABLE_ID, Collections.singletonList("notes")));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .containsExactly(new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.TRY_EVOLVE,
                                new DropTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .containsExactly(new DropTableEvent(NORMALIZE_TEST_TABLE_ID));
    }

    @Test
    void testNormalizeSchemaChangeEventsInLenientMode() {

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new CreateTableEvent(
                                        NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA)))
                .containsExactly(
                        new CreateTableEvent(NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new AddColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "added_flag", DataTypes.BOOLEAN()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "id")))))
                .containsExactly(
                        new AddColumnEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "added_flag", DataTypes.BOOLEAN()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new AlterColumnTypeEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", DataTypes.DOUBLE()))))
                .containsExactly(
                        new AlterColumnTypeEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonMap("age", DataTypes.DOUBLE()),
                                Collections.singletonMap("age", DataTypes.FLOAT())));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new RenameColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", "aging"))))
                .containsExactly(
                        new AddColumnEvent(
                                NORMALIZE_TEST_TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("aging", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new DropColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList("notes"))))
                .isEmpty();

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .containsExactly(new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.LENIENT,
                                new DropTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .containsExactly(new DropTableEvent(NORMALIZE_TEST_TABLE_ID));
    }

    @Test
    void testNormalizeSchemaChangeEventsInIgnoreMode() {

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new CreateTableEvent(
                                        NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA)))
                .containsExactly(
                        new CreateTableEvent(NORMALIZE_TEST_TABLE_ID, NORMALIZE_TEST_SCHEMA));

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new AddColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "added_flag", DataTypes.BOOLEAN()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "id")))))
                .isEmpty();

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new AlterColumnTypeEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", DataTypes.DOUBLE()))))
                .isEmpty();

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new RenameColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonMap("age", "aging"))))
                .isEmpty();

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new DropColumnEvent(
                                        NORMALIZE_TEST_TABLE_ID,
                                        Collections.singletonList("notes"))))
                .isEmpty();

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new TruncateTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .isEmpty();

        assertThat(
                        normalizeEvent(
                                SchemaChangeBehavior.IGNORE,
                                new DropTableEvent(NORMALIZE_TEST_TABLE_ID)))
                .isEmpty();
    }

    @Test
    void testDeduceMergedCreateTableEvent() {
        TableIdRouter router =
                new TableIdRouter(
                        Arrays.asList(
                                // Simple 1-to-1 routing rules
                                new RouteRule("db_1.table_1", "db_1.table_1"),
                                // Re-routed rules
                                new RouteRule("db_2.table_1", "db_2.table_2"),
                                // Merging tables
                                new RouteRule("db_3.table_\\.*", "db_3.table_merged"),
                                // Broadcast tables
                                new RouteRule("db_4.table_1", "db_4.table_a"),
                                new RouteRule("db_4.table_1", "db_4.table_b")));
        List<CreateTableEvent> createTableEvents =
                Arrays.asList(
                        new CreateTableEvent(
                                TableId.parse("db_1.table_1"),
                                Schema.newBuilder()
                                        .physicalColumn("id1", DataTypes.INT())
                                        .physicalColumn("name1", DataTypes.VARCHAR(128))
                                        .physicalColumn("age1", DataTypes.FLOAT())
                                        .physicalColumn("notes1", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_2.table_1"),
                                Schema.newBuilder()
                                        .physicalColumn("id2", DataTypes.INT())
                                        .physicalColumn("name2", DataTypes.VARCHAR(128))
                                        .physicalColumn("age2", DataTypes.FLOAT())
                                        .physicalColumn("notes2", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_3.table_1"),
                                Schema.newBuilder()
                                        .physicalColumn("id", DataTypes.INT())
                                        .physicalColumn("name", DataTypes.VARCHAR(128))
                                        .physicalColumn("age", DataTypes.FLOAT())
                                        .physicalColumn("notes", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_3.table_2"),
                                Schema.newBuilder()
                                        .physicalColumn("id", DataTypes.INT())
                                        .physicalColumn("name", DataTypes.VARCHAR(128))
                                        .physicalColumn("age", DataTypes.FLOAT())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_3.table_3"),
                                Schema.newBuilder()
                                        .physicalColumn("id", DataTypes.BIGINT())
                                        .physicalColumn("name", DataTypes.VARCHAR(200))
                                        .physicalColumn("age", DataTypes.FLOAT())
                                        .physicalColumn("notes", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_4.table_1"),
                                Schema.newBuilder()
                                        .physicalColumn("id4", DataTypes.INT())
                                        .physicalColumn("name4", DataTypes.VARCHAR(128))
                                        .physicalColumn("age4", DataTypes.FLOAT())
                                        .physicalColumn("notes4", DataTypes.STRING())
                                        .build()));
        List<CreateTableEvent> mergedCreateTableEvents =
                SchemaDerivator.deduceMergedCreateTableEvent(router, createTableEvents);
        assertThat(mergedCreateTableEvents)
                .containsExactly(
                        new CreateTableEvent(
                                TableId.parse("db_3.table_merged"),
                                Schema.newBuilder()
                                        .physicalColumn("id", DataTypes.BIGINT())
                                        .physicalColumn("name", DataTypes.STRING())
                                        .physicalColumn("age", DataTypes.FLOAT())
                                        .physicalColumn("notes", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_4.table_a"),
                                Schema.newBuilder()
                                        .physicalColumn("id4", DataTypes.INT())
                                        .physicalColumn("name4", DataTypes.VARCHAR(128))
                                        .physicalColumn("age4", DataTypes.FLOAT())
                                        .physicalColumn("notes4", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_4.table_b"),
                                Schema.newBuilder()
                                        .physicalColumn("id4", DataTypes.INT())
                                        .physicalColumn("name4", DataTypes.VARCHAR(128))
                                        .physicalColumn("age4", DataTypes.FLOAT())
                                        .physicalColumn("notes4", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_1.table_1"),
                                Schema.newBuilder()
                                        .physicalColumn("id1", DataTypes.INT())
                                        .physicalColumn("name1", DataTypes.VARCHAR(128))
                                        .physicalColumn("age1", DataTypes.FLOAT())
                                        .physicalColumn("notes1", DataTypes.STRING())
                                        .build()),
                        new CreateTableEvent(
                                TableId.parse("db_2.table_2"),
                                Schema.newBuilder()
                                        .physicalColumn("id2", DataTypes.INT())
                                        .physicalColumn("name2", DataTypes.VARCHAR(128))
                                        .physicalColumn("age2", DataTypes.FLOAT())
                                        .physicalColumn("notes2", DataTypes.STRING())
                                        .build()));
    }
}
