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

package com.ververica.cdc.common.testutils.assertions;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
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
