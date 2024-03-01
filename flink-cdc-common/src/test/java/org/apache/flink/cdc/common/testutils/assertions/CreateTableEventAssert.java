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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;

/** Assertions for {@link CreateTableEvent}. */
public class CreateTableEventAssert
        extends ChangeEventAssert<CreateTableEventAssert, CreateTableEvent> {

    public static CreateTableEventAssert assertThatCreateTableEvent(CreateTableEvent event) {
        return new CreateTableEventAssert(event);
    }

    protected CreateTableEventAssert(CreateTableEvent event) {
        super(event, CreateTableEventAssert.class);
    }

    public CreateTableEventAssert hasSchema(Schema schema) {
        isNotNull();
        if (!actual.getSchema().equals(schema)) {
            failWithActualExpectedAndMessage(
                    actual.getSchema(),
                    schema,
                    "The schema of the CreateTableEvent is not as expected");
        }
        return myself;
    }
}
