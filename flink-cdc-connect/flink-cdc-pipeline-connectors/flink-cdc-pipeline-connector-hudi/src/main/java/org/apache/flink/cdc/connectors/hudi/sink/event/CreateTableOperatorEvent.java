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

package org.apache.flink.cdc.connectors.hudi.sink.event;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * An operator event that encapsulates a {@link CreateTableEvent}.
 *
 * <p>This event is sent from the {@code MultiTableEventStreamWriteFunction} to the {@code
 * MultiTableStreamWriteOperatorCoordinator} to signal that a new table has been discovered in the
 * CDC stream. The coordinator uses this event to initialize all necessary resources for the new
 * table, such as its dedicated write client and event buffers, before any data is written.
 */
public class CreateTableOperatorEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final CreateTableEvent createTableEvent;

    /**
     * Constructs a new CreateTableOperatorEvent.
     *
     * @param createTableEvent The original CDC event that triggered this operator event.
     */
    public CreateTableOperatorEvent(CreateTableEvent createTableEvent) {
        this.createTableEvent = createTableEvent;
    }

    /**
     * Gets the encapsulated {@link CreateTableEvent}.
     *
     * @return The original create table event.
     */
    public CreateTableEvent getCreateTableEvent() {
        return createTableEvent;
    }

    @Override
    public String toString() {
        return "CreateTableOperatorEvent{" + "tableId=" + createTableEvent.tableId() + '}';
    }
}
