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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.paimon.sink.utils.TypeUtils;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriterHelper;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.RowAssignerChannelComputer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link HashFunction} implementation for {@link PaimonDataSink}. Shuffle {@link DataChangeEvent}
 * by hash of PrimaryKey.
 *
 * <p>Table type (append-only vs. primary-key) is inferred directly from the CDC {@link Schema}
 * instead of querying the Paimon catalog. This avoids a {@code TableNotExistException} when the
 * target table has not yet been created by {@code MetadataApplier}, which can happen in distributed
 * pipeline topologies where pre-partitioning precedes schema coordination.
 */
public class PaimonHashFunction implements HashFunction<DataChangeEvent>, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<RecordData.FieldGetter> fieldGetters;

    private final RowAssignerChannelComputer channelComputer;

    private final int parallelism;

    public PaimonHashFunction(Schema schema, ZoneId zoneId, int parallelism) {
        this.parallelism = parallelism;
        if (schema.primaryKeys().isEmpty()) {
            // Append-only table: spread events randomly across subtasks.
            this.fieldGetters = null;
            this.channelComputer = null;
        } else {
            this.fieldGetters = PaimonWriterHelper.createFieldGetters(schema, zoneId);
            TableSchema tableSchema = buildTableSchema(schema);
            this.channelComputer = new RowAssignerChannelComputer(tableSchema, parallelism);
            this.channelComputer.setup(parallelism);
        }
    }

    @Override
    public int hashcode(DataChangeEvent event) {
        if (channelComputer != null) {
            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(event, fieldGetters);
            return channelComputer.channel(genericRow);
        } else {
            // Avoid sending all events to the same subtask when table has no primary key.
            return ThreadLocalRandom.current().nextInt(parallelism);
        }
    }

    private static TableSchema buildTableSchema(Schema schema) {
        List<Column> columns = schema.getColumns();
        List<DataField> dataFields = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            dataFields.add(
                    new DataField(i, col.getName(), TypeUtils.toPaimonDataType(col.getType())));
        }
        return new TableSchema(
                0L,
                dataFields,
                dataFields.size() - 1,
                schema.partitionKeys(),
                schema.primaryKeys(),
                Collections.emptyMap(),
                "");
    }
}
