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

import org.apache.flink.cdc.common.event.DropColumnEvent;

import org.assertj.core.internal.Iterables;

import java.util.List;

/** Assertions for {@link DropColumnEvent}. */
public class DropColumnEventAssert
        extends ChangeEventAssert<DropColumnEventAssert, DropColumnEvent> {
    private final Iterables iterables = Iterables.instance();

    public static DropColumnEventAssert assertThatDropColumnEvent(DropColumnEvent event) {
        return new DropColumnEventAssert(event);
    }

    private DropColumnEventAssert(DropColumnEvent event) {
        super(event, DropColumnEventAssert.class);
    }

    public DropColumnEventAssert containsDroppedColumns(String... droppedColumns) {
        List<String> actualDroppedColumns = actual.getDroppedColumnNames();
        iterables.assertContainsExactlyInAnyOrder(info, actualDroppedColumns, droppedColumns);
        return myself;
    }
}
