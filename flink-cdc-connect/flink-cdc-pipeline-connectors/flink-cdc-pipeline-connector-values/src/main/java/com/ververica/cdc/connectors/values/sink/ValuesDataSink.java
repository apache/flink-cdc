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

package com.ververica.cdc.connectors.values.sink;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.connectors.values.ValuesDatabase;
import com.ververica.cdc.runtime.state.TableSchemaState;
import com.ververica.cdc.runtime.state.TableSchemaStateSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link DataSink} for "values" connector that supports schema evolution. */
@Internal
public class ValuesDataSink implements DataSink, Serializable {

    /** {@link ValuesDataSinkOptions#MATERIALIZED_IN_MEMORY}. */
    private final boolean materializedInMemory;

    public ValuesDataSink(boolean materializedInMemory) {
        this.materializedInMemory = materializedInMemory;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new ValuesSink(materializedInMemory));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new ValuesDatabase.ValuesMetadataApplier();
    }

    /** an e2e {@link Sink} implementation that print all {@link DataChangeEvent} out. */
    private static class ValuesSink implements StatefulSink<Event, TableSchemaState> {

        private final boolean materializedInMemory;

        public ValuesSink(boolean materializedInMemory) {
            this.materializedInMemory = materializedInMemory;
        }

        @Override
        public StatefulSinkWriter<Event, TableSchemaState> createWriter(InitContext context) {
            final ValuesSinkWriter writer = new ValuesSinkWriter(materializedInMemory);
            writer.open(context.getSubtaskId(), context.getNumberOfParallelSubtasks());
            return writer;
        }

        @Override
        public StatefulSinkWriter<Event, TableSchemaState> restoreWriter(
                InitContext context, Collection<TableSchemaState> recoveredState) {
            final ValuesSinkWriter writer = new ValuesSinkWriter(materializedInMemory);
            writer.initializeState(recoveredState);
            return writer;
        }

        @Override
        public SimpleVersionedSerializer<TableSchemaState> getWriterStateSerializer() {
            return new TableSchemaStateSerializer();
        }
    }

    /**
     * Print {@link DataChangeEvent} to console, and update table records in {@link ValuesDatabase}.
     */
    private static class ValuesSinkWriter extends PrintSinkOutputWriter<Event>
            implements StatefulSink.StatefulSinkWriter<Event, TableSchemaState> {

        private final boolean materializedInMemory;

        /**
         * keep the relationship of TableId and Schema as write method may rely on the schema
         * information of DataChangeEvent.
         */
        private final Map<TableId, Schema> schemaMaps;

        public ValuesSinkWriter(boolean materializedInMemory) {
            super();
            this.materializedInMemory = materializedInMemory;
            schemaMaps = new HashMap<>();
        }

        private void initializeState(Collection<TableSchemaState> bucketStates) {
            bucketStates.forEach(
                    TableSchemaState ->
                            schemaMaps.put(
                                    TableSchemaState.getTableId(), TableSchemaState.getSchema()));
        }

        @Override
        public void write(Event event) {
            super.write(event);
            if (event instanceof SchemaChangeEvent) {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                TableId tableId = schemaChangeEvent.tableId();
                if (event instanceof CreateTableEvent) {
                    schemaMaps.put(tableId, ((CreateTableEvent) event).getSchema());
                } else {
                    if (!schemaMaps.containsKey(tableId)) {
                        throw new RuntimeException("schema of " + tableId + " is not existed.");
                    }
                    schemaMaps.put(
                            tableId,
                            SchemaUtils.applySchemaChangeEvent(
                                    schemaMaps.get(tableId), schemaChangeEvent));
                }
            } else if (materializedInMemory && event instanceof DataChangeEvent) {
                ValuesDatabase.applyDataChangeEvent((DataChangeEvent) event);
            }
        }

        @Override
        public List<TableSchemaState> snapshotState(long checkpointId) {
            List<TableSchemaState> states = new ArrayList<>();
            for (Map.Entry<TableId, Schema> entry : schemaMaps.entrySet()) {
                states.add(new TableSchemaState(entry.getKey(), entry.getValue()));
            }
            return states;
        }
    }
}
