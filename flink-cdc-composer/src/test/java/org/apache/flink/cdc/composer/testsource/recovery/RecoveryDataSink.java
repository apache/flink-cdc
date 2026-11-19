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

package org.apache.flink.cdc.composer.testsource.recovery;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** A values sink whose first metadata application fails after applying the first added column. */
public class RecoveryDataSink implements DataSink {

    private static final AtomicBoolean FAILED = new AtomicBoolean();
    private static final AtomicInteger FAILURES = new AtomicInteger();
    private static final AtomicInteger FIRST_COLUMN_APPLICATIONS = new AtomicInteger();

    public static void reset() {
        FAILED.set(false);
        FAILURES.set(0);
        FIRST_COLUMN_APPLICATIONS.set(0);
    }

    public static int getFailures() {
        return FAILURES.get();
    }

    public static int getFirstColumnApplications() {
        return FIRST_COLUMN_APPLICATIONS.get();
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return new ValuesDataSink(true, false, ValuesDataSink.SinkApi.SINK_FUNCTION, false)
                .getEventSinkProvider();
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new FailingOnceMetadataApplier();
    }

    private static class FailingOnceMetadataApplier implements MetadataApplier {

        private final ValuesDatabase.ValuesMetadataApplier delegate =
                new ValuesDatabase.ValuesMetadataApplier(true);

        @Override
        public MetadataApplier setAcceptedSchemaEvolutionTypes(
                Set<SchemaChangeEventType> schemaEvolutionTypes) {
            delegate.setAcceptedSchemaEvolutionTypes(schemaEvolutionTypes);
            return this;
        }

        @Override
        public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
            return delegate.acceptsSchemaEvolutionType(schemaChangeEventType);
        }

        @Override
        public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
            return delegate.getSupportedSchemaEvolutionTypes();
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            if (schemaChangeEvent instanceof AddColumnEvent
                    && ((AddColumnEvent) schemaChangeEvent)
                            .getAddedColumns()
                            .get(0)
                            .getAddColumn()
                            .getName()
                            .equals("extra_v1")) {
                FIRST_COLUMN_APPLICATIONS.incrementAndGet();
                if (FAILED.compareAndSet(false, true)) {
                    delegate.applySchemaChange(schemaChangeEvent);
                    FAILURES.incrementAndGet();
                    throw new RuntimeException("Fail after applying extra_v1 without rollback.");
                }
                return;
            }
            delegate.applySchemaChange(schemaChangeEvent);
        }
    }
}
