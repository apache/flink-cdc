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
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;

/** A test for the {@link org.apache.flink.cdc.common.utils.ChangeEventUtils}. */
public class ChangeEventUtilsTest {
    @Test
    public void testResolveSchemaEvolutionOptions() {
        Assert.assertEquals(
                Sets.set(
                        TRUNCATE_TABLE,
                        RENAME_COLUMN,
                        CREATE_TABLE,
                        DROP_TABLE,
                        ALTER_COLUMN_TYPE,
                        ADD_COLUMN,
                        DROP_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.emptyList(), Collections.emptyList()));

        Assert.assertEquals(
                Sets.set(
                        ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN, CREATE_TABLE, TRUNCATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.emptyList(), Collections.singletonList("drop")));

        Assert.assertEquals(
                Sets.set(ADD_COLUMN, CREATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Arrays.asList("create", "add"), Collections.emptyList()));

        Assert.assertEquals(
                Sets.set(ADD_COLUMN, ALTER_COLUMN_TYPE, RENAME_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.singletonList("column"),
                        Collections.singletonList("drop.column")));

        Assert.assertEquals(
                Sets.set(
                        ADD_COLUMN,
                        DROP_TABLE,
                        TRUNCATE_TABLE,
                        RENAME_COLUMN,
                        ALTER_COLUMN_TYPE,
                        CREATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionOptions(
                        Collections.emptyList(), Collections.singletonList("drop.column")));
    }

    @Test
    public void testResolveSchemaEvolutionTag() {
        Assert.assertEquals(
                Arrays.asList(
                        ADD_COLUMN,
                        ALTER_COLUMN_TYPE,
                        CREATE_TABLE,
                        DROP_COLUMN,
                        DROP_TABLE,
                        RENAME_COLUMN,
                        TRUNCATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionTag("all"));

        Assert.assertEquals(
                Arrays.asList(ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionTag("column"));

        Assert.assertEquals(
                Arrays.asList(CREATE_TABLE, DROP_TABLE, TRUNCATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionTag("table"));

        Assert.assertEquals(
                Collections.singletonList(RENAME_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionTag("rename.column"));

        Assert.assertEquals(
                Arrays.asList(DROP_COLUMN, DROP_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionTag("drop"));

        Assert.assertEquals(
                Collections.singletonList(DROP_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionTag("drop.column"));

        Assert.assertEquals(
                Collections.singletonList(CREATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionTag("create"));

        Assert.assertEquals(
                Collections.singletonList(CREATE_TABLE),
                ChangeEventUtils.resolveSchemaEvolutionTag("create.table"));

        Assert.assertEquals(
                Arrays.asList(ALTER_COLUMN_TYPE),
                ChangeEventUtils.resolveSchemaEvolutionTag("alter"));

        Assert.assertEquals(
                Collections.singletonList(ALTER_COLUMN_TYPE),
                ChangeEventUtils.resolveSchemaEvolutionTag("alter.column.type"));

        Assert.assertEquals(
                Collections.singletonList(ADD_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionTag("add"));

        Assert.assertEquals(
                Collections.singletonList(ADD_COLUMN),
                ChangeEventUtils.resolveSchemaEvolutionTag("add.column"));
    }
}
