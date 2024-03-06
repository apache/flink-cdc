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

package org.apache.flink.cdc.common.testutils.assertions;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

/** Assertions for {@link SchemaChangeEvent}. */
public class SchemaChangeEventAssert
        extends ChangeEventAssert<SchemaChangeEventAssert, SchemaChangeEvent> {

    public static SchemaChangeEventAssert assertThatSchemaChangeEvent(SchemaChangeEvent event) {
        return new SchemaChangeEventAssert(event);
    }

    protected SchemaChangeEventAssert(SchemaChangeEvent event) {
        super(event, SchemaChangeEventAssert.class);
    }

    public CreateTableEventAssert asCreateTableEvent() {
        isInstanceOf(CreateTableEvent.class);
        return CreateTableEventAssert.assertThatCreateTableEvent((CreateTableEvent) actual);
    }

    public AddColumnEventAssert asAddColumnEvent() {
        isInstanceOf(AddColumnEvent.class);
        return AddColumnEventAssert.assertThatAddColumnEvent((AddColumnEvent) actual);
    }

    public DropColumnEventAssert asDropColumnEvent() {
        isInstanceOf(DropColumnEvent.class);
        return DropColumnEventAssert.assertThatDropColumnEvent((DropColumnEvent) actual);
    }

    public RenameColumnEventAssert asRenameColumnEvent() {
        isInstanceOf(RenameColumnEvent.class);
        return RenameColumnEventAssert.assertThatRenameColumnEvent(((RenameColumnEvent) actual));
    }

    public AlterColumnTypeEventAssert asAlterColumnTypeEvent() {
        isInstanceOf(AlterColumnTypeEvent.class);
        return AlterColumnTypeEventAssert.assertThatAlterColumnTypeEvent(
                ((AlterColumnTypeEvent) actual));
    }
}
