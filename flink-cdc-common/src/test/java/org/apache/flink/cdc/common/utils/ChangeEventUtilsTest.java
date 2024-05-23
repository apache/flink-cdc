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

import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.emptyList(), Collections.emptyList()),
                Sets.set(CREATE_TABLE, ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.emptyList(), Collections.singletonList("drop")),
                Sets.set(CREATE_TABLE, ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Arrays.asList("create", "add"), Collections.emptyList()),
                Sets.set(CREATE_TABLE, ADD_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.singletonList("column"),
                        Collections.singletonList("drop.column")),
                Sets.set(ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.emptyList(), Collections.singletonList("drop.column")),
                Sets.set(CREATE_TABLE, ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN));
    }

    @Test
    public void testResolveSchemaEvolutionTag() {
        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("all"),
                Arrays.asList(
                        ADD_COLUMN, CREATE_TABLE, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("column"),
                Arrays.asList(ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("table"),
                Collections.singletonList(CREATE_TABLE));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("rename"),
                Collections.singletonList(RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("rename.column"),
                Collections.singletonList(RENAME_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("drop"),
                Collections.singletonList(DROP_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("drop.column"),
                Collections.singletonList(DROP_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("create"),
                Collections.singletonList(CREATE_TABLE));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("create.table"),
                Collections.singletonList(CREATE_TABLE));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("alter"),
                Collections.singletonList(ALTER_COLUMN_TYPE));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("alter.column.type"),
                Collections.singletonList(ALTER_COLUMN_TYPE));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("add"),
                Collections.singletonList(ADD_COLUMN));

        Assert.assertEquals(
                ChangeEventUtils.resolveSchemaEvolutionTag("add.column"),
                Collections.singletonList(ADD_COLUMN));
    }
}
