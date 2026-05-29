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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.TargetTableCreateMode;
import org.apache.flink.table.api.ValidationException;

import java.util.Set;

/** A {@link MetadataApplier} wrapper that applies the configured target table create mode. */
@Internal
public class TargetTableCreateModeMetadataApplier implements MetadataApplier {

    private final MetadataApplier delegate;
    private final TargetTableCreateMode targetTableCreateMode;

    private TargetTableCreateModeMetadataApplier(
            MetadataApplier delegate, TargetTableCreateMode targetTableCreateMode) {
        this.delegate = delegate;
        this.targetTableCreateMode = targetTableCreateMode;
    }

    public static MetadataApplier wrap(
            MetadataApplier delegate, TargetTableCreateMode targetTableCreateMode, String sinkType) {
        if (targetTableCreateMode == TargetTableCreateMode.CREATE_IF_NOT_EXISTS) {
            return delegate;
        }
        if (!(delegate instanceof SupportsTargetTableExistenceCheck)) {
            throw new ValidationException(
                    String.format(
                            "Sink '%s' does not support '%s=%s'. The sink metadata applier must implement %s.",
                            sinkType,
                            SinkOptions.TARGET_TABLE_CREATE_MODE.key(),
                            targetTableCreateMode,
                            SupportsTargetTableExistenceCheck.class.getSimpleName()));
        }
        return new TargetTableCreateModeMetadataApplier(delegate, targetTableCreateMode);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (targetTableCreateMode == TargetTableCreateMode.ERROR_IF_NOT_EXISTS
                && schemaChangeEvent instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) schemaChangeEvent;
            SupportsTargetTableExistenceCheck existenceCheck =
                    (SupportsTargetTableExistenceCheck) delegate;
            if (!existenceCheck.targetTableExists(createTableEvent.tableId())) {
                throw new ValidationException(
                        "Target table does not exist: " + createTableEvent.tableId());
            }
        }
        delegate.applySchemaChange(schemaChangeEvent);
    }

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
    public void close() throws Exception {
        delegate.close();
    }
}
