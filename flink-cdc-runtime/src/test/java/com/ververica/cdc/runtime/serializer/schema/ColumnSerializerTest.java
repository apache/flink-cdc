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

package com.ververica.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

/** A test for the {@link ColumnSerializer}. */
public class ColumnSerializerTest extends SerializerTestBase<Column> {
    @Override
    protected TypeSerializer<Column> createSerializer() {
        return ColumnSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Column> getTypeClass() {
        return Column.class;
    }

    @Override
    protected Column[] getTestData() {
        return new Column[] {
            Column.physicalColumn("col1", DataTypes.BIGINT()),
            Column.physicalColumn("col1", DataTypes.BIGINT(), "comment"),
            Column.metadataColumn("col1", DataTypes.BIGINT()),
            Column.metadataColumn("col1", DataTypes.BIGINT(), "mKey"),
            Column.metadataColumn("col1", DataTypes.BIGINT(), "mKey", "comment")
        };
    }
}
