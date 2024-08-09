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

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;

/** A test for the {@link org.apache.flink.cdc.common.utils.ChangeEventUtils}. */
public class ChangeEventUtilsTest {
    @Test
    public void testResolveSchemaEvolutionOptions() {
        Assertions.assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Collections.emptyList(), Collections.emptyList()))
                .isEqualTo(
                        Sets.set(
                                CREATE_TABLE,
                                ADD_COLUMN,
                                ALTER_COLUMN_TYPE,
                                DROP_COLUMN,
                                RENAME_COLUMN));

        Assertions.assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Collections.emptyList(), Collections.singletonList("drop")))
                .isEqualTo(Sets.set(CREATE_TABLE, ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));

        Assertions.assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Arrays.asList("create", "add"), Collections.emptyList()))
                .isEqualTo(Sets.set(CREATE_TABLE, ADD_COLUMN));

        Assertions.assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Collections.singletonList("column"),
                                Collections.singletonList("drop.column")))
                .isEqualTo(Sets.set(ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));

        Assertions.assertThat(
                        ChangeEventUtils.resolveSchemaEvolutionOptions(
                                Collections.emptyList(), Collections.singletonList("drop.column")))
                .isEqualTo(Sets.set(CREATE_TABLE, ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));
    }

    @Test
    public void testResolveSchemaEvolutionTag() {
        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("all"))
                .isEqualTo(
                        Arrays.asList(
                                ADD_COLUMN,
                                CREATE_TABLE,
                                ALTER_COLUMN_TYPE,
                                DROP_COLUMN,
                                RENAME_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("column"))
                .isEqualTo(
                        Arrays.asList(ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("table"))
                .isEqualTo(Collections.singletonList(CREATE_TABLE));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("rename"))
                .isEqualTo(Collections.singletonList(RENAME_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("rename.column"))
                .isEqualTo(Collections.singletonList(RENAME_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("drop"))
                .isEqualTo(Collections.singletonList(DROP_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("drop.column"))
                .isEqualTo(Collections.singletonList(DROP_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("create"))
                .isEqualTo(Collections.singletonList(CREATE_TABLE));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("create.table"))
                .isEqualTo(Collections.singletonList(CREATE_TABLE));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("alter"))
                .isEqualTo(Collections.singletonList(ALTER_COLUMN_TYPE));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("alter.column.type"))
                .isEqualTo(Collections.singletonList(ALTER_COLUMN_TYPE));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("add"))
                .isEqualTo(Collections.singletonList(ADD_COLUMN));

        Assertions.assertThat(ChangeEventUtils.resolveSchemaEvolutionTag("add.column"))
                .isEqualTo(Collections.singletonList(ADD_COLUMN));
    }
}
