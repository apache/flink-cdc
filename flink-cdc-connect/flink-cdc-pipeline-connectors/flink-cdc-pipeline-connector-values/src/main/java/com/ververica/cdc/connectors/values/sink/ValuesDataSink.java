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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.ChangeEvent;
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link DataSink} for "values" connector that supports schema evolution. */
@Internal
public class ValuesDataSink implements DataSink, Serializable {

    /** {@link ValuesDataSinkOptions#MATERIALIZED_IN_MEMORY}. */
    private final boolean materializedInMemory;

    private final boolean print;

    public ValuesDataSink(boolean materializedInMemory, boolean print) {
        this.materializedInMemory = materializedInMemory;
        this.print = print;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new ValuesSink(materializedInMemory, print));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new ValuesDatabase.ValuesMetadataApplier();
    }

    /** an e2e {@link Sink} implementation that print all {@link DataChangeEvent} out. */
    private static class ValuesSink implements Sink<Event> {

        private final boolean materializedInMemory;

        private final boolean print;

        public ValuesSink(boolean materializedInMemory, boolean print) {
            this.materializedInMemory = materializedInMemory;
            this.print = print;
        }

        @Override
        public SinkWriter<Event> createWriter(InitContext context) {
            return new ValuesSinkWriter(
                    materializedInMemory,
                    print,
                    context.getSubtaskId(),
                    context.getNumberOfParallelSubtasks());
        }
    }

    /**
     * Print {@link DataChangeEvent} to console, and update table records in {@link ValuesDatabase}.
     */
    private static class ValuesSinkWriter implements SinkWriter<Event> {

        private final boolean materializedInMemory;

        private final boolean print;

        private final int subtaskIndex;

        private final int numSubtasks;

        /**
         * keep the relationship of TableId and Schema as write method may rely on the schema
         * information of DataChangeEvent.
         */
        private final Map<TableId, Schema> schemaMaps;

        private final Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps;

        public ValuesSinkWriter(
                boolean materializedInMemory, boolean print, int subtaskIndex, int numSubtasks) {
            super();
            this.materializedInMemory = materializedInMemory;
            this.print = print;
            this.subtaskIndex = subtaskIndex;
            this.numSubtasks = numSubtasks;
            schemaMaps = new HashMap<>();
            fieldGetterMaps = new HashMap<>();
        }

        @Override
        public void write(Event event, Context context) {
            if (event instanceof SchemaChangeEvent) {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                TableId tableId = schemaChangeEvent.tableId();
                if (event instanceof CreateTableEvent) {
                    Schema schema = ((CreateTableEvent) event).getSchema();
                    schemaMaps.put(tableId, schema);
                    fieldGetterMaps.put(tableId, SchemaUtils.createFieldGetters(schema));
                } else {
                    if (!schemaMaps.containsKey(tableId)) {
                        throw new RuntimeException("schema of " + tableId + " is not existed.");
                    }
                    Schema schema =
                            SchemaUtils.applySchemaChangeEvent(
                                    schemaMaps.get(tableId), schemaChangeEvent);
                    schemaMaps.put(tableId, schema);
                    fieldGetterMaps.put(tableId, SchemaUtils.createFieldGetters(schema));
                }
            } else if (materializedInMemory && event instanceof DataChangeEvent) {
                ValuesDatabase.applyDataChangeEvent((DataChangeEvent) event);
            }
            if (print) {
                String prefix = numSubtasks > 1 ? subtaskIndex + "> " : "";
                // print the detail message to console for verification.
                System.out.println(
                        prefix
                                + ValuesDataSinkHelper.convertEventToStr(
                                        event,
                                        fieldGetterMaps.get(((ChangeEvent) event).tableId())));
            }
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
    }
}
