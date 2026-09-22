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
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.ExistingTableSchemaExpansionMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.ExistingTableSchemaExpansionSupport;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExistingTableSchemaExpander}. */
class ExistingTableSchemaExpanderTest {

    private static final TableId TABLE_ID = TableId.tableId("inventory", "products");

    @Test
    void testDoesNothingWhenTargetTableDoesNotExist() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(null);

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(eventWithMissingColumn());
        assertThat(applier.queryCalls).isOne();
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testAddsMissingColumnsAsNullableIdempotently() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT);
        CreateTableEvent event =
                createTableEvent(
                        Column.physicalColumn("id", DataTypes.INT()),
                        Column.physicalColumn("name", DataTypes.STRING().notNull()));

        expander.handleExistingTableCreation(event);
        expander.handleExistingTableCreation(event);

        assertThat(applier.appliedEvents).hasSize(1);
        AddColumnEvent addColumnEvent = (AddColumnEvent) applier.appliedEvents.get(0);
        assertThat(addColumnEvent.getAddedColumns())
                .extracting(AddColumnEvent.ColumnWithPosition::getAddColumn)
                .containsExactly(Column.physicalColumn("name", DataTypes.STRING()));
    }

    @Test
    void testDoesNotAddMissingPrimaryOrPartitionKeys() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", DataTypes.STRING())));
        Schema pipelineSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("region", DataTypes.STRING().notNull())
                        .physicalColumn("value", DataTypes.STRING())
                        .primaryKey("id")
                        .partitionKey("region")
                        .build();

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(new CreateTableEvent(TABLE_ID, pipelineSchema));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDoesNothingWhenTargetTypeIsWider() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", DataTypes.BIGINT())));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", DataTypes.INT())));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testWidensNarrowTargetTypeIdempotently() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", DataTypes.INT())));
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT);
        CreateTableEvent event =
                createTableEvent(Column.physicalColumn("value", DataTypes.BIGINT()));

        expander.handleExistingTableCreation(event);
        expander.handleExistingTableCreation(event);

        assertThat(applier.appliedEvents).singleElement().isInstanceOf(AlterColumnTypeEvent.class);
        AlterColumnTypeEvent alterColumnTypeEvent =
                (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(alterColumnTypeEvent.getTypeMapping())
                .containsExactly(Map.entry("value", DataTypes.BIGINT()));
        assertThat(alterColumnTypeEvent.getOldTypeMapping())
                .containsExactly(Map.entry("value", DataTypes.INT()));
    }

    @Test
    void testDoesNotWidenKeyColumns() {
        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        Schema pipelineSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(targetSchema);

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(new CreateTableEvent(TABLE_ID, pipelineSchema));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testWidensDecimalWithoutReducingExistingIntegerRange() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("amount", DataTypes.DECIMAL(12, 2))));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(
                                Column.physicalColumn("amount", DataTypes.DECIMAL(12, 4))));

        AlterColumnTypeEvent event = (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(event.getTypeMapping())
                .containsExactly(Map.entry("amount", DataTypes.DECIMAL(14, 4)));
    }

    @ParameterizedTest
    @MethodSource("safeWideningCases")
    void testSafeWideningFamilies(
            DataType targetType, DataType pipelineType, DataType expectedWidenedType) {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", targetType)));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", pipelineType)));

        AlterColumnTypeEvent event = (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(event.getTypeMapping()).containsExactly(Map.entry("value", expectedWidenedType));
    }

    @ParameterizedTest
    @MethodSource("unsafeWideningCases")
    void testDelegatesTypesWithoutConservativeWidening(DataType targetType, DataType pipelineType) {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", targetType)));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", pipelineType)));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDelegatesCompletelyIncompatibleTypes() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", DataTypes.BOOLEAN())));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", DataTypes.TIMESTAMP())));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testNormalizationAvoidsUnimplementableAlter() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("event_time", DataTypes.TIMESTAMP(6))));
        applier.normalizer =
                (existingSchema, type) -> {
                    if (type instanceof TimestampType) {
                        return DataTypes.TIMESTAMP(
                                        Math.min(((TimestampType) type).getPrecision(), 6))
                                .copy(type.isNullable());
                    }
                    return type;
                };

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(
                                Column.physicalColumn("event_time", DataTypes.TIMESTAMP(9))));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testNormalizationReceivesExistingTargetTableOptions() {
        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .options(Collections.singletonMap("integer-width", "32"))
                        .build();
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(targetSchema);
        applier.normalizer =
                (existingSchema, type) ->
                        "32".equals(existingSchema.options().get("integer-width"))
                                        && type.equals(DataTypes.BIGINT())
                                ? DataTypes.INT()
                                : type;

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("id", DataTypes.BIGINT())));

        assertThat(applier.appliedEvents).isEmpty();
        assertThat(applier.queryCalls).isOne();
    }

    @Test
    void testNormalizationFailureIsIsolatedPerColumn() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(
                                Column.physicalColumn("broken", DataTypes.INT()),
                                Column.physicalColumn("id", DataTypes.INT())));
        applier.normalizationFailureColumns.add("broken");

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(
                                Column.physicalColumn("broken", DataTypes.BIGINT()),
                                Column.physicalColumn("id", DataTypes.BIGINT()),
                                Column.physicalColumn("description", DataTypes.STRING())));
        assertThat(applier.appliedEvents)
                .extracting(SchemaChangeEvent::getType)
                .containsExactly(
                        SchemaChangeEventType.ADD_COLUMN, SchemaChangeEventType.ALTER_COLUMN_TYPE);
        AlterColumnTypeEvent alterColumnTypeEvent =
                (AlterColumnTypeEvent) applier.appliedEvents.get(1);
        assertThat(alterColumnTypeEvent.getTypeMapping())
                .containsExactly(Map.entry("id", DataTypes.BIGINT()));
    }

    @Test
    void testMatchesColumnNamesCaseInsensitively() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("VALUE", DataTypes.INT())));
        applier.columnNameCaseSensitive = false;

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", DataTypes.BIGINT())));

        AlterColumnTypeEvent alterColumnTypeEvent =
                (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(alterColumnTypeEvent.getTypeMapping())
                .containsExactly(Map.entry("VALUE", DataTypes.BIGINT()));
    }

    @Test
    void testKeepsExactMatchingForCaseSensitiveTarget() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("VALUE", DataTypes.INT())));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", DataTypes.BIGINT())));
        assertThat(applier.appliedEvents).singleElement().isInstanceOf(AddColumnEvent.class);
    }

    @Test
    void testDelegatesAmbiguousCaseInsensitiveColumnNames() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(
                                Column.physicalColumn("Value", DataTypes.INT()),
                                Column.physicalColumn("VALUE", DataTypes.INT())));
        applier.columnNameCaseSensitive = false;

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", DataTypes.BIGINT())));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDelegatesNullableToNotNullDifference() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("value", DataTypes.BIGINT().notNull())));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("value", DataTypes.BIGINT())));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDelegatesWhenExpansionTypeIsUnavailable() {
        TestingExistingTableSchemaExpansionSupport addUnavailableApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        addUnavailableApplier.acceptedEventTypes =
                Collections.singleton(SchemaChangeEventType.CREATE_TABLE);

        new ExistingTableSchemaExpander(
                        addUnavailableApplier, addUnavailableApplier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(eventWithMissingColumn());
        assertThat(addUnavailableApplier.appliedEvents).isEmpty();
        assertThat(addUnavailableApplier.queryCalls).isZero();

        TestingExistingTableSchemaExpansionSupport alterUnavailableApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        alterUnavailableApplier.supportedEventTypes =
                Collections.singleton(SchemaChangeEventType.ADD_COLUMN);

        new ExistingTableSchemaExpander(
                        alterUnavailableApplier,
                        alterUnavailableApplier,
                        SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("id", DataTypes.BIGINT())));
        assertThat(alterUnavailableApplier.appliedEvents).isEmpty();
        assertThat(alterUnavailableApplier.queryCalls).isOne();
    }

    @Test
    void testDoesNotQueryTargetWhenNeitherExpansionDdlIsSupported() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        applier.supportedEventTypes = Collections.singleton(SchemaChangeEventType.CREATE_TABLE);

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(eventWithMissingColumn());

        assertThat(applier.queryCalls).isZero();
        assertThat(applier.appliedEvents).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaChangeBehavior.class,
            names = {"IGNORE", "EXCEPTION"})
    void testDoesNotExpandWhenSchemaChangeBehaviorDisablesEvolution(
            SchemaChangeBehavior schemaChangeBehavior) {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        CreateTableEvent event =
                createTableEvent(
                        Column.physicalColumn("id", DataTypes.BIGINT()),
                        Column.physicalColumn("name", DataTypes.STRING()));

        new ExistingTableSchemaExpander(applier, applier, schemaChangeBehavior)
                .handleExistingTableCreation(event);
        assertThat(applier.appliedEvents).isEmpty();
        assertThat(applier.queryCalls).isZero();
    }

    @Test
    void testDelegatesToSinkOnNormalizationFailure() {
        TestingExistingTableSchemaExpansionSupport normalizationFailureApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        normalizationFailureApplier.normalizer =
                (existingSchema, type) -> {
                    throw new RuntimeException("Expected normalization failure");
                };
        assertThatCode(
                        () ->
                                new ExistingTableSchemaExpander(
                                                normalizationFailureApplier,
                                                normalizationFailureApplier,
                                                SchemaChangeBehavior.LENIENT)
                                        .handleExistingTableCreation(
                                                createTableEvent(
                                                        Column.physicalColumn(
                                                                "id", DataTypes.BIGINT()))))
                .doesNotThrowAnyException();
        assertThat(normalizationFailureApplier.appliedEvents).isEmpty();
    }

    @Test
    void testExpandFailsFastOnTargetSchemaQueryFailure() {
        TestingExistingTableSchemaExpansionSupport queryFailureApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        queryFailureApplier.queryFailureAfterCalls = 0;
        ExistingTableSchemaExpander queryFailureExpander =
                new ExistingTableSchemaExpander(
                        queryFailureApplier,
                        queryFailureApplier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.EXPAND);
        assertThatThrownBy(
                        () ->
                                queryFailureExpander.handleExistingTableCreation(
                                        eventWithMissingColumn()))
                .isInstanceOf(FlinkRuntimeException.class);
        assertThat(queryFailureApplier.appliedEvents).isEmpty();
    }

    @Test
    void testExpandFailsFastOnTransientDdlFailure() {
        TestingExistingTableSchemaExpansionSupport ddlFailureApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        ddlFailureApplier.failedEventTypes.add(SchemaChangeEventType.ADD_COLUMN);
        assertThatThrownBy(
                        () ->
                                new ExistingTableSchemaExpander(
                                                ddlFailureApplier,
                                                ddlFailureApplier,
                                                SchemaChangeBehavior.LENIENT,
                                                ExistingTableSchemaExpansionMode.EXPAND)
                                        .handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOf(FlinkRuntimeException.class);
        assertThat(ddlFailureApplier.queryCalls).isOne();
    }

    @Test
    void testTryExpandFailsFastOnUnsupportedDdlFailure() {
        TestingExistingTableSchemaExpansionSupport unsupportedApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        unsupportedApplier.unsupportedEventTypes.add(SchemaChangeEventType.ADD_COLUMN);
        assertThatThrownBy(
                        () ->
                                new ExistingTableSchemaExpander(
                                                unsupportedApplier,
                                                unsupportedApplier,
                                                SchemaChangeBehavior.LENIENT)
                                        .handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class);
        assertThat(unsupportedApplier.appliedEvents).isEmpty();
    }

    @Test
    void testTryExpandFailsFastOnTransientDdlFailure() {
        // After supportable differences are identified, a transient DDL failure must propagate so
        // the job fails over and retries, instead of silently delegating to the sink and dropping
        // the missing columns forever.
        TestingExistingTableSchemaExpansionSupport ddlFailureApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        ddlFailureApplier.failedEventTypes.add(SchemaChangeEventType.ADD_COLUMN);

        assertThatThrownBy(
                        () ->
                                new ExistingTableSchemaExpander(
                                                ddlFailureApplier,
                                                ddlFailureApplier,
                                                SchemaChangeBehavior.LENIENT,
                                                ExistingTableSchemaExpansionMode.TRY_EXPAND)
                                        .handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOf(SchemaEvolveException.class);
        assertThat(ddlFailureApplier.appliedEvents).isEmpty();
    }

    @Test
    void testTryExpandFailsFastOnReadBackQueryFailure() {
        // The initial read succeeds and derived DDL is issued, but the read-back verification query
        // fails transiently. This must propagate for failover instead of being swallowed.
        TestingExistingTableSchemaExpansionSupport readBackFailureApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        readBackFailureApplier.queryFailureAfterCalls = 1;

        assertThatThrownBy(
                        () ->
                                new ExistingTableSchemaExpander(
                                                readBackFailureApplier,
                                                readBackFailureApplier,
                                                SchemaChangeBehavior.LENIENT,
                                                ExistingTableSchemaExpansionMode.TRY_EXPAND)
                                        .handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOf(FlinkRuntimeException.class);
        assertThat(readBackFailureApplier.appliedEvents).hasSize(1);
    }

    @Test
    void testVerifiesExpansionByReadingBackTargetSchema() {
        TestingExistingTableSchemaExpansionSupport unchangedTargetApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        unchangedTargetApplier.updateTargetSchema = false;

        new ExistingTableSchemaExpander(
                        unchangedTargetApplier,
                        unchangedTargetApplier,
                        SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(eventWithMissingColumn());
        assertThat(unchangedTargetApplier.appliedEvents)
                .singleElement()
                .isInstanceOf(AddColumnEvent.class);
        // One initial query plus one read-back verification after the derived DDL.
        assertThat(unchangedTargetApplier.queryCalls).isEqualTo(2);
        assertThat(unchangedTargetApplier.targetSchema.getColumn("name")).isEmpty();

        TestingExistingTableSchemaExpansionSupport unchangedTargetTypeApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        unchangedTargetTypeApplier.updateTargetSchema = false;
        new ExistingTableSchemaExpander(
                        unchangedTargetTypeApplier,
                        unchangedTargetTypeApplier,
                        SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(Column.physicalColumn("id", DataTypes.BIGINT())));
        assertThat(unchangedTargetTypeApplier.appliedEvents)
                .singleElement()
                .isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(unchangedTargetTypeApplier.queryCalls).isEqualTo(2);
        assertThat(unchangedTargetTypeApplier.targetSchema.getColumn("id"))
                .get()
                .extracting(Column::getType)
                .isEqualTo(DataTypes.INT());
    }

    @Test
    void testCheckPassesWithoutIssuingDdl() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        // CHECK validates compatibility regardless of whether DDL is enabled or supported.
        applier.acceptedEventTypes = EnumSet.noneOf(SchemaChangeEventType.class);
        applier.supportedEventTypes = EnumSet.noneOf(SchemaChangeEventType.class);

        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.CHECK);
        assertThat(
                        expander.handleExistingTableCreation(
                                createTableEvent(Column.physicalColumn("id", DataTypes.INT()))))
                .isFalse();
        assertThat(applier.appliedEvents).isEmpty();
        assertThat(applier.queryCalls).isOne();
    }

    @Test
    void testCheckFailsOnMissingOrNarrowColumns() {
        // Missing column: CHECK never issues DDL, so an addable column is still a failure.
        TestingExistingTableSchemaExpansionSupport missingColumnApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        ExistingTableSchemaExpander missingColumnExpander =
                new ExistingTableSchemaExpander(
                        missingColumnApplier,
                        missingColumnApplier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.CHECK);
        assertThatThrownBy(
                        () ->
                                missingColumnExpander.handleExistingTableCreation(
                                        eventWithMissingColumn()))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e ->
                                assertThat(e.getExceptionMessage())
                                        .contains("missing column")
                                        .contains("name")
                                        .contains("ADD COLUMN"));
        assertThat(missingColumnApplier.appliedEvents).isEmpty();

        // Narrow column: SMALLINT can be safely widened, but CHECK cannot issue DDL.
        TestingExistingTableSchemaExpansionSupport narrowColumnApplier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.SMALLINT())));
        ExistingTableSchemaExpander narrowColumnExpander =
                new ExistingTableSchemaExpander(
                        narrowColumnApplier,
                        narrowColumnApplier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.CHECK);
        assertThatThrownBy(
                        () ->
                                narrowColumnExpander.handleExistingTableCreation(
                                        createTableEvent(
                                                Column.physicalColumn("id", DataTypes.INT()))))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e ->
                                assertThat(e.getExceptionMessage())
                                        .contains("narrower")
                                        .contains("id")
                                        .contains("ALTER COLUMN"));
        assertThat(narrowColumnApplier.appliedEvents).isEmpty();
    }

    @Test
    void testExpandFailsWhenNoExpansionDdlIsAvailable() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        applier.supportedEventTypes = EnumSet.noneOf(SchemaChangeEventType.class);
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.EXPAND);
        assertThatThrownBy(() -> expander.handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e -> assertThat(e.getExceptionMessage()).contains("ADD_COLUMN"));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testExpandSucceedsWhenCompatibleEvenWithoutDdlSupport() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        applier.supportedEventTypes = EnumSet.noneOf(SchemaChangeEventType.class);
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.EXPAND);
        assertThat(
                        expander.handleExistingTableCreation(
                                createTableEvent(Column.physicalColumn("id", DataTypes.INT()))))
                .isTrue();
        assertThat(applier.appliedEvents).isEmpty();
        assertThat(applier.queryCalls).isOne();
    }

    @Test
    void testCheckRunsEvenUnderIgnoreAndExceptionBehaviors() {
        // CHECK guards the initial table state and is independent of schema.change.behavior.
        for (SchemaChangeBehavior behavior :
                Arrays.asList(SchemaChangeBehavior.IGNORE, SchemaChangeBehavior.EXCEPTION)) {
            TestingExistingTableSchemaExpansionSupport applier =
                    new TestingExistingTableSchemaExpansionSupport(
                            schema(Column.physicalColumn("id", DataTypes.STRING())));
            ExistingTableSchemaExpander expander =
                    new ExistingTableSchemaExpander(
                            applier, applier, behavior, ExistingTableSchemaExpansionMode.CHECK);
            assertThatThrownBy(
                            () ->
                                    expander.handleExistingTableCreation(
                                            createTableEvent(
                                                    Column.physicalColumn("id", DataTypes.INT()))))
                    .isInstanceOf(SchemaEvolveException.class);
        }
    }

    @Test
    void testCheckFailsOnMissingTargetTable() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(null);
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.CHECK);
        assertThatThrownBy(() -> expander.handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e ->
                                assertThat(e.getExceptionMessage())
                                        .contains("does not exist")
                                        .contains("CHECK mode never creates tables"));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testCheckFailsWithAggregatedIncompatibilities() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(
                                Column.physicalColumn("id", DataTypes.STRING()),
                                Column.physicalColumn("ts", DataTypes.INT())));
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.CHECK);
        assertThatThrownBy(
                        () ->
                                expander.handleExistingTableCreation(
                                        createTableEvent(
                                                Column.physicalColumn("id", DataTypes.INT()),
                                                Column.physicalColumn(
                                                        "ts", DataTypes.TIMESTAMP()))))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e -> {
                            assertThat(e.getExceptionMessage()).contains("id");
                            assertThat(e.getExceptionMessage()).contains("ts");
                        });
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testExpandFailsWhenRequiredDdlIsUnsupported() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        applier.supportedEventTypes = EnumSet.of(SchemaChangeEventType.ALTER_COLUMN_TYPE);
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.EXPAND);
        assertThatThrownBy(() -> expander.handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e ->
                                assertThat(e.getExceptionMessage())
                                        .contains("ADD_COLUMN")
                                        .contains("name"));
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testExpandFailsOnNoOpDdl() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        // The derived DDL call returns normally but does not mutate the target schema.
        applier.updateTargetSchema = false;
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.EXPAND);
        assertThatThrownBy(() -> expander.handleExistingTableCreation(eventWithMissingColumn()))
                .isInstanceOfSatisfying(
                        SchemaEvolveException.class,
                        e -> assertThat(e.getExceptionMessage()).contains("after expansion"));
        assertThat(applier.appliedEvents).singleElement().isInstanceOf(AddColumnEvent.class);
    }

    @Test
    void testOffModeLeavesTargetUntouched() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        ExistingTableSchemaExpander expander =
                new ExistingTableSchemaExpander(
                        applier,
                        applier,
                        SchemaChangeBehavior.LENIENT,
                        ExistingTableSchemaExpansionMode.DISABLED);
        assertThat(expander.handleExistingTableCreation(eventWithMissingColumn())).isTrue();
        assertThat(applier.queryCalls).isZero();
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testAppliesAddAndAlterInOneExpansion() {
        TestingExistingTableSchemaExpansionSupport applier =
                new TestingExistingTableSchemaExpansionSupport(
                        schema(Column.physicalColumn("id", DataTypes.INT())));

        new ExistingTableSchemaExpander(applier, applier, SchemaChangeBehavior.LENIENT)
                .handleExistingTableCreation(
                        createTableEvent(
                                Column.physicalColumn("id", DataTypes.BIGINT()),
                                Column.physicalColumn(
                                        "description", DataTypes.STRING().notNull())));
        assertThat(applier.appliedEvents)
                .extracting(SchemaChangeEvent::getType)
                .containsExactly(
                        SchemaChangeEventType.ADD_COLUMN, SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    private static CreateTableEvent eventWithMissingColumn() {
        return createTableEvent(
                Column.physicalColumn("id", DataTypes.INT()),
                Column.physicalColumn("name", DataTypes.STRING()));
    }

    private static CreateTableEvent createTableEvent(Column... columns) {
        return new CreateTableEvent(TABLE_ID, schema(columns));
    }

    private static Schema schema(Column... columns) {
        return Schema.newBuilder().setColumns(Arrays.asList(columns)).build();
    }

    private static Stream<Arguments> safeWideningCases() {
        return Stream.of(
                Arguments.arguments(
                        DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.SMALLINT()),
                Arguments.arguments(DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.INT()),
                Arguments.arguments(DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                Arguments.arguments(DataTypes.CHAR(4), DataTypes.CHAR(8), DataTypes.CHAR(8)),
                Arguments.arguments(DataTypes.CHAR(8), DataTypes.VARCHAR(4), DataTypes.VARCHAR(8)),
                Arguments.arguments(
                        DataTypes.VARCHAR(4), DataTypes.VARCHAR(8), DataTypes.VARCHAR(8)),
                Arguments.arguments(DataTypes.BINARY(4), DataTypes.BINARY(8), DataTypes.BINARY(8)),
                Arguments.arguments(
                        DataTypes.BINARY(8), DataTypes.VARBINARY(4), DataTypes.VARBINARY(8)),
                Arguments.arguments(
                        DataTypes.VARBINARY(4), DataTypes.VARBINARY(8), DataTypes.VARBINARY(8)),
                Arguments.arguments(DataTypes.TIME(3), DataTypes.TIME(6), DataTypes.TIME(6)),
                Arguments.arguments(
                        DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(6)),
                Arguments.arguments(
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.TIMESTAMP_LTZ(6)),
                Arguments.arguments(
                        DataTypes.TIMESTAMP_TZ(3),
                        DataTypes.TIMESTAMP_TZ(6),
                        DataTypes.TIMESTAMP_TZ(6)));
    }

    private static Stream<Arguments> unsafeWideningCases() {
        return Stream.of(
                Arguments.arguments(DataTypes.BIGINT(), DataTypes.FLOAT()),
                Arguments.arguments(DataTypes.DECIMAL(38, 0), DataTypes.DECIMAL(38, 38)),
                Arguments.arguments(DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP_LTZ(6)),
                Arguments.arguments(DataTypes.VARCHAR(8), DataTypes.VARBINARY(8)));
    }

    private static class TestingExistingTableSchemaExpansionSupport
            implements MetadataApplier, ExistingTableSchemaExpansionSupport {

        private Schema targetSchema;
        private Set<SchemaChangeEventType> acceptedEventTypes =
                EnumSet.allOf(SchemaChangeEventType.class);
        private Set<SchemaChangeEventType> supportedEventTypes =
                EnumSet.allOf(SchemaChangeEventType.class);
        private final Set<SchemaChangeEventType> failedEventTypes =
                EnumSet.noneOf(SchemaChangeEventType.class);
        private final Set<SchemaChangeEventType> unsupportedEventTypes =
                EnumSet.noneOf(SchemaChangeEventType.class);
        private final List<SchemaChangeEvent> appliedEvents = new ArrayList<>();
        private final Set<String> normalizationFailureColumns = new java.util.HashSet<>();
        private boolean updateTargetSchema = true;
        private int queryFailureAfterCalls = Integer.MAX_VALUE;
        private int queryCalls;
        private BiFunction<Schema, DataType, DataType> normalizer = (schema, type) -> type;
        private boolean columnNameCaseSensitive = true;

        private TestingExistingTableSchemaExpansionSupport(Schema targetSchema) {
            this.targetSchema = targetSchema;
        }

        @Override
        public Optional<Schema> getExistingTableSchema(TableId tableId) {
            if (queryCalls++ >= queryFailureAfterCalls) {
                throw new RuntimeException("Expected target schema query failure");
            }
            return Optional.ofNullable(targetSchema);
        }

        @Override
        public DataType normalizeToTargetDataType(
                TableId tableId,
                String columnName,
                DataType pipelineDataType,
                Schema existingTargetSchema) {
            if (normalizationFailureColumns.contains(columnName)) {
                throw new RuntimeException("Expected normalization failure");
            }
            return normalizer.apply(existingTargetSchema, pipelineDataType);
        }

        @Override
        public boolean isColumnNameCaseSensitive() {
            return columnNameCaseSensitive;
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
                throws SchemaEvolveException {
            if (unsupportedEventTypes.contains(schemaChangeEvent.getType())) {
                throw new UnsupportedSchemaChangeEventException(schemaChangeEvent);
            }
            if (failedEventTypes.contains(schemaChangeEvent.getType())) {
                throw new SchemaEvolveException(schemaChangeEvent, "Expected DDL failure");
            }
            appliedEvents.add(schemaChangeEvent);
            if (updateTargetSchema) {
                targetSchema = SchemaUtils.applySchemaChangeEvent(targetSchema, schemaChangeEvent);
            }
        }

        @Override
        public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
            return acceptedEventTypes.contains(schemaChangeEventType);
        }

        @Override
        public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
            return supportedEventTypes;
        }
    }
}
