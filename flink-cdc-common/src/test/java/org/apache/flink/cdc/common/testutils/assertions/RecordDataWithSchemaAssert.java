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

package org.apache.flink.cdc.common.testutils.assertions;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.assertj.core.internal.Iterables;

import java.util.ArrayList;
import java.util.List;

/**
 * Assertions for {@link RecordData} with schema so that fields in the RecordData can be checked.
 */
public class RecordDataWithSchemaAssert extends RecordDataAssert<RecordDataWithSchemaAssert> {
    private final Schema schema;
    private final Iterables iterables = Iterables.instance();

    protected RecordDataWithSchemaAssert(RecordData recordData, Schema schema) {
        super(recordData, RecordDataWithSchemaAssert.class);
        this.schema = schema;
    }

    public RecordDataWithSchemaAssert hasFields(Object... fields) {
        objects.assertNotNull(info, schema);
        List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(schema);
        List<Object> actualFields = new ArrayList<>();
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            actualFields.add(fieldGetter.getFieldOrNull(actual));
        }
        iterables.assertContainsExactly(info, actualFields, fields);
        return myself;
    }
}
