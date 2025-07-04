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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;

import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;
import static org.apache.flink.cdc.common.testutils.assertions.EventAssertions.assertThat;

/** A test for the {@link org.apache.flink.cdc.common.utils.ChangeEventUtils}. */
class ChangeEventUtilsTest {
    @Test
    void testResolveSchemaEvolutionOptions() {

        List<String> allTags =
                Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                        .map(SchemaChangeEventType::getTag)
                        .collect(Collectors.toList());
        assertThat(ChangeEventUtils.resolveSchemaEvolutionOptions(allTags, Collections.emptyList()))
                .isEqualTo(
                        Sets.set(
                                TRUNCATE_TABLE,
                                RENAME_COLUMN,
                                CREATE_TABLE,
                                DROP_TABLE,
                                ALTER_COLUMN_TYPE,
                                ADD_COLUMN,
                                DROP_COLUMN));

        assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                allTags, Collections.singletonList("drop")))
                .isEqualTo(
                        Sets.set(
                                ADD_COLUMN,
                                ALTER_COLUMN_TYPE,
                                RENAME_COLUMN,
                                CREATE_TABLE,
                                TRUNCATE_TABLE));

        assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Arrays.asList("create", "add"), Collections.emptyList()))
                .isEqualTo(Sets.set(ADD_COLUMN, CREATE_TABLE));

        assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Collections.singletonList("column"),
                                Collections.singletonList("drop.column")))
                .isEqualTo(Sets.set(ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));

        assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                allTags, Collections.singletonList("drop.column")))
                .isEqualTo(
                        Sets.set(
                                ADD_COLUMN,
                                DROP_TABLE,
                                TRUNCATE_TABLE,
                                RENAME_COLUMN,
                                ALTER_COLUMN_TYPE,
                                CREATE_TABLE));
    }

    @Test
    void testResolveSchemaEvolutionTag() {
        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("all"))
                .isEqualTo(
                        Arrays.asList(
                                ADD_COLUMN,
                                ALTER_COLUMN_TYPE,
                                CREATE_TABLE,
                                DROP_COLUMN,
                                DROP_TABLE,
                                RENAME_COLUMN,
                                TRUNCATE_TABLE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("column"))
                .isEqualTo(
                        Arrays.asList(ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("table"))
                .isEqualTo(Arrays.asList(CREATE_TABLE, DROP_TABLE, TRUNCATE_TABLE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("rename.column"))
                .isEqualTo(Collections.singletonList(RENAME_COLUMN));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("drop"))
                .isEqualTo(Arrays.asList(DROP_COLUMN, DROP_TABLE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("drop.column"))
                .isEqualTo(Collections.singletonList(DROP_COLUMN));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("create"))
                .isEqualTo(Collections.singletonList(CREATE_TABLE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("create.table"))
                .isEqualTo(Collections.singletonList(CREATE_TABLE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("alter"))
                .isEqualTo(Collections.singletonList(ALTER_COLUMN_TYPE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("alter.column.type"))
                .isEqualTo(Collections.singletonList(ALTER_COLUMN_TYPE));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("add"))
                .isEqualTo(Collections.singletonList(ADD_COLUMN));

        assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("add.column"))
                .isEqualTo(Collections.singletonList(ADD_COLUMN));
    }
}
