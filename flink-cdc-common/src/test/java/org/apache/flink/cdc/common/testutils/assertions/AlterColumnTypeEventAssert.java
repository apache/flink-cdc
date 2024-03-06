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

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.types.DataType;

import org.assertj.core.internal.Maps;

import java.util.Map;

/** Assertions for {@link AlterColumnTypeEvent}. */
public class AlterColumnTypeEventAssert
        extends ChangeEventAssert<AlterColumnTypeEventAssert, AlterColumnTypeEvent> {
    private final Maps maps = Maps.instance();

    public static AlterColumnTypeEventAssert assertThatAlterColumnTypeEvent(
            AlterColumnTypeEvent event) {
        return new AlterColumnTypeEventAssert(event);
    }

    private AlterColumnTypeEventAssert(AlterColumnTypeEvent event) {
        super(event, AlterColumnTypeEventAssert.class);
    }

    public AlterColumnTypeEventAssert containsTypeMapping(Map<String, DataType> typeMapping) {
        Map<String, DataType> actualTypeMapping = actual.getTypeMapping();
        maps.assertContainsAllEntriesOf(info, actualTypeMapping, typeMapping);
        return myself;
    }
}
