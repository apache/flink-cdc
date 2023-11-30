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

import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.TableId;
import org.assertj.core.api.AbstractAssert;

/** Assertions for {@link ChangeEvent}. */
public class ChangeEventAssert<SELF extends AbstractAssert<SELF, EVENT>, EVENT extends ChangeEvent>
        extends AbstractAssert<SELF, EVENT> {

    public static <SELF extends ChangeEventAssert<SELF, ChangeEvent>>
            ChangeEventAssert<SELF, ChangeEvent> assertThatChangeEvent(ChangeEvent changeEvent) {
        return new ChangeEventAssert<>(changeEvent, ChangeEventAssert.class);
    }

    protected ChangeEventAssert(EVENT event, Class<?> selfType) {
        super(event, selfType);
    }

    public SELF hasTableId(TableId tableId) {
        if (!actual.tableId().equals(tableId)) {
            failWithActualExpectedAndMessage(
                    actual.tableId(),
                    tableId,
                    "Table ID of the DataChangeEvent is not as expected");
        }
        return myself;
    }
}
