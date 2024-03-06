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

package org.apache.flink.cdc.debezium.event;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.EventDeserializer;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Deserializer to deserialize {@link SourceRecord} to {@link Event}. */
@Internal
public abstract class SourceRecordEventDeserializer implements EventDeserializer<SourceRecord> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SourceRecordEventDeserializer.class);

    @Override
    public List<? extends Event> deserialize(SourceRecord record) throws Exception {
        if (isDataChangeRecord(record)) {
            LOG.trace("Process data change record: {}", record);
            return deserializeDataChangeRecord(record);
        } else if (isSchemaChangeRecord(record)) {
            LOG.trace("Process schema change record: {}", record);
            return deserializeSchemaChangeRecord(record);
        } else {
            LOG.trace("Ignored other record: {}", record);
            return Collections.emptyList();
        }
    }

    /** Whether the given record is a data change record. */
    protected abstract boolean isDataChangeRecord(SourceRecord record);

    /** Whether the given record is a schema change record. */
    protected abstract boolean isSchemaChangeRecord(SourceRecord record);

    /** Deserialize given data change record to {@link DataChangeEvent}. */
    protected abstract List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record)
            throws Exception;

    /** Deserialize given schema change record to {@link SchemaChangeEvent}. */
    protected abstract List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record)
            throws Exception;

    /** Get {@link TableId} from data change record. */
    protected abstract TableId getTableId(SourceRecord record);

    /** Get metadata from data change record. */
    protected abstract Map<String, String> getMetadata(SourceRecord record);

    public static Schema fieldSchema(Schema schema, String fieldName) {
        return schema.field(fieldName).schema();
    }

    public static Struct fieldStruct(Struct value, String fieldName) {
        return value.getStruct(fieldName);
    }
}
