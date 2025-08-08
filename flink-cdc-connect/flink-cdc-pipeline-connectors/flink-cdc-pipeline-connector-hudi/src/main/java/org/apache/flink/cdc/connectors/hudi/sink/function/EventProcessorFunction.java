/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.function;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

/**
 * Template interface for processing CDC events in a standardized way. Provides a consistent event
 * handling pattern across different write function implementations.
 *
 * <p>All write functions should implement this interface to ensure uniform event processing with
 * clear separation of concerns:
 *
 * <ul>
 *   <li>{@link #processDataChange(DataChangeEvent)} - Handles INSERT/UPDATE/DELETE operations
 *   <li>{@link #processSchemaChange(SchemaChangeEvent)} - Handles DDL operations (CREATE TABLE,
 *       ADD COLUMN, etc.)
 *   <li>{@link #processFlush(FlushEvent)} - Handles coordinated flushing of buffered data
 * </ul>
 */
public interface EventProcessorFunction {

    /**
     * Process data change events (INSERT/UPDATE/DELETE). This is where actual data is buffered and
     * written.
     *
     * @param event The data change event
     * @throws Exception if processing fails
     */
    void processDataChange(DataChangeEvent event) throws Exception;

    /**
     * Process schema change events (CREATE TABLE, ADD COLUMN, etc.).
     *
     * @param event The schema change event
     * @throws Exception if processing fails
     */
    void processSchemaChange(SchemaChangeEvent event) throws Exception;

    /**
     * Process flush events for coordinated flushing.
     *
     * @param event The flush event
     * @throws Exception if processing fails
     */
    void processFlush(FlushEvent event) throws Exception;
}
