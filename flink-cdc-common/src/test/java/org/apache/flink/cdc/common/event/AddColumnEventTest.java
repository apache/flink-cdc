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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AddColumnEvent}. */
class AddColumnEventTest {

    @Test
    @SuppressWarnings("deprecation")
    void testLegacyColumnPositionConvertsToAndFromTopLevelPosition() {
        for (AddColumnEvent.ColumnPosition legacyPosition :
                AddColumnEvent.ColumnPosition.values()) {
            ColumnPosition position = legacyPosition.toColumnPosition();

            assertThat(position.name()).isEqualTo(legacyPosition.name());
            assertThat(AddColumnEvent.ColumnPosition.fromColumnPosition(position))
                    .isEqualTo(legacyPosition);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void testLegacyColumnPositionConversionAcceptsNullTopLevelPosition() {
        assertThat(AddColumnEvent.ColumnPosition.fromColumnPosition(null)).isNull();
    }

    @Test
    @SuppressWarnings("deprecation")
    void testExistingFactoriesAndPositionGetterKeepUsingLegacyPosition() {
        Column column = Column.physicalColumn("column", DataTypes.STRING());

        assertThat(AddColumnEvent.first(column).getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.FIRST);
        assertThat(AddColumnEvent.last(column).getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.LAST);
        assertThat(AddColumnEvent.before(column, "reference").getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.BEFORE);
        assertThat(AddColumnEvent.after(column, "reference").getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.AFTER);
    }

    @Test
    void testTopLevelColumnPositionMigrationPath() {
        Column column = Column.physicalColumn("column", DataTypes.STRING());

        AddColumnEvent.ColumnWithPosition value =
                AddColumnEvent.ColumnWithPosition.of(column, ColumnPosition.AFTER, "reference");

        assertThat(value.getAddColumn()).isEqualTo(column);
        assertThat(value.getColumnPosition()).isEqualTo(ColumnPosition.AFTER);
        assertThat(value.getReferenceColumnName()).isEqualTo("reference");
    }

    @Test
    void testTopLevelFactoryRejectsInvalidArguments() {
        Column column = Column.physicalColumn("column", DataTypes.STRING());

        assertThatThrownBy(
                        () ->
                                AddColumnEvent.ColumnWithPosition.of(
                                        null, ColumnPosition.FIRST, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AddColumnEvent.ColumnWithPosition.of(column, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                AddColumnEvent.ColumnWithPosition.of(
                                        column, ColumnPosition.BEFORE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                AddColumnEvent.ColumnWithPosition.of(
                                        column, ColumnPosition.AFTER, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                AddColumnEvent.ColumnWithPosition.of(
                                        column, ColumnPosition.FIRST, "reference"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                AddColumnEvent.ColumnWithPosition.of(
                                        column, ColumnPosition.LAST, "reference"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                AddColumnEvent.ColumnWithPosition.of(
                                        column, ColumnPosition.BEFORE, "column"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("deprecation")
    void testLegacyColumnWithPositionApiRemainsDeprecatedAndCompatible() throws Exception {
        AddColumnEvent.ColumnWithPosition legacy =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("column", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.BEFORE,
                        "reference");
        assertThat(legacy.getPosition()).isEqualTo(AddColumnEvent.ColumnPosition.BEFORE);
        assertThat(legacy.getExistedColumnName()).isEqualTo("reference");
    }
}
