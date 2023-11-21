/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.state;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/** A test for the {@link TableSchemaStateSerializer}. */
public class TableSchemaStateSerializerTest {

    @Test
    public void testDeserialize() throws IOException {
        TableSchemaStateSerializer serializer = new TableSchemaStateSerializer();
        Schema schema =
                new Schema.Builder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        TableSchemaState tableSchemaState =
                new TableSchemaState(TableId.parse("default.default.table1"), schema);
        byte[] bytes = serializer.serialize(tableSchemaState);
        Assert.assertEquals(
                tableSchemaState,
                serializer.deserialize(TableSchemaStateSerializer.DEFAULT_VERSION, bytes));
    }
}
