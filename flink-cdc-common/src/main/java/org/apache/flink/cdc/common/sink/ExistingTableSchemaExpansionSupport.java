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

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.io.Serializable;
import java.util.Optional;

/** Connector-specific capabilities required to safely expand an existing target table schema. */
@Experimental
public interface ExistingTableSchemaExpansionSupport extends Serializable {

    /**
     * Returns the current schema of the target table, or {@link Optional#empty()} if the table does
     * not exist. Target physical types must be converted to the same canonical CDC representation
     * produced by {@link #normalizeToTargetDataType(TableId, String, DataType, Schema)}. The
     * returned schema should include any table options that affect physical type mapping.
     */
    Optional<Schema> getExistingTableSchema(TableId tableId) throws SchemaEvolveException;

    /**
     * Converts a pipeline type to the canonical CDC representation of the physical type that this
     * applier will create in the existing target table. For example, if both {@code CHAR(n)} and
     * {@code VARCHAR(n)} map to target {@code VARCHAR(n)}, this method should return the same
     * {@code VARCHAR(n)} representation for both input types. The existing target schema provides
     * table options needed by sinks whose type mapping depends on table properties.
     *
     * <p>This method must not query or modify the target table.
     */
    DataType normalizeToTargetDataType(
            TableId tableId,
            String columnName,
            DataType pipelineDataType,
            Schema existingTargetSchema);

    /** Returns whether target column names are matched case-sensitively. */
    boolean isColumnNameCaseSensitive();
}
