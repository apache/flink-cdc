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

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;

/** Assertions for {@link DataChangeEvent}. */
public class DataChangeEventAssert
        extends ChangeEventAssert<DataChangeEventAssert, DataChangeEvent> {

    public static DataChangeEventAssert assertThatDataChangeEvent(DataChangeEvent event) {
        return new DataChangeEventAssert(event);
    }

    protected DataChangeEventAssert(DataChangeEvent dataChangeEvent) {
        super(dataChangeEvent, DataChangeEventAssert.class);
    }

    public DataChangeEventAssert hasOperationType(OperationType operationType) {
        if (!actual.op().equals(operationType)) {
            failWithMessage(
                    "Expect DataChangeEvent to have operation type \"%s\", but was \"%s\"",
                    operationType, actual.op());
        }
        return this;
    }

    public RecordDataAssert<?> withBeforeRecordData() {
        if (actual.before() == null) {
            failWithMessage(
                    "DataChangeEvent with operation type \"%s\" does not have \"before\" field",
                    actual.op());
        }
        return RecordDataAssert.assertThatRecordData(actual.before());
    }

    public RecordDataAssert<?> withAfterRecordData() {
        if (actual.after() == null) {
            failWithMessage(
                    "DataChangeEvent with operation type \"%s\" does not have \"after\" field",
                    actual.op());
        }
        return RecordDataAssert.assertThatRecordData(actual.after());
    }
}
