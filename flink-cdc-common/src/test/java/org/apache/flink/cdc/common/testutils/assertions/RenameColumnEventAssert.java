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

import org.apache.flink.cdc.common.event.RenameColumnEvent;

import org.assertj.core.internal.Maps;

import java.util.Map;

/** Assertions for {@link RenameColumnEvent}. */
public class RenameColumnEventAssert
        extends ChangeEventAssert<RenameColumnEventAssert, RenameColumnEvent> {
    private final Maps maps = Maps.instance();

    public static RenameColumnEventAssert assertThatRenameColumnEvent(RenameColumnEvent event) {
        return new RenameColumnEventAssert(event);
    }

    private RenameColumnEventAssert(RenameColumnEvent event) {
        super(event, RenameColumnEventAssert.class);
    }

    public RenameColumnEventAssert containsNameMapping(Map<String, String> nameMapping) {
        Map<String, String> actualNameMapping = actual.getNameMapping();
        maps.assertContainsAllEntriesOf(info, actualNameMapping, nameMapping);
        return myself;
    }
}
