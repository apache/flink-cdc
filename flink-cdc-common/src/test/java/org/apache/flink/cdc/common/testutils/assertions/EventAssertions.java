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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import org.assertj.core.api.Assertions;

/** Collection for entries of customized event-related assertions. */
public class EventAssertions extends Assertions {
    public static EventAssert assertThat(Event event) {
        return EventAssert.assertThatEvent(event);
    }

    public static <SELF extends ChangeEventAssert<SELF, ChangeEvent>>
            ChangeEventAssert<SELF, ChangeEvent> assertThat(ChangeEvent changeEvent) {
        return ChangeEventAssert.assertThatChangeEvent(changeEvent);
    }

    public static DataChangeEventAssert assertThat(DataChangeEvent event) {
        return DataChangeEventAssert.assertThatDataChangeEvent(event);
    }

    public static <SELF extends RecordDataAssert<SELF>> RecordDataAssert<SELF> assertThat(
            RecordData recordData) {
        return RecordDataAssert.assertThatRecordData(recordData);
    }

    public static SchemaChangeEventAssert assertThat(SchemaChangeEvent event) {
        return SchemaChangeEventAssert.assertThatSchemaChangeEvent(event);
    }

    public static CreateTableEventAssert assertThat(CreateTableEvent event) {
        return CreateTableEventAssert.assertThatCreateTableEvent(event);
    }

    public static AddColumnEventAssert assertThat(AddColumnEvent event) {
        return AddColumnEventAssert.assertThatAddColumnEvent(event);
    }

    public static DropColumnEventAssert assertThat(DropColumnEvent event) {
        return DropColumnEventAssert.assertThatDropColumnEvent(event);
    }

    public static RenameColumnEventAssert assertThat(RenameColumnEvent event) {
        return RenameColumnEventAssert.assertThatRenameColumnEvent(event);
    }
}
