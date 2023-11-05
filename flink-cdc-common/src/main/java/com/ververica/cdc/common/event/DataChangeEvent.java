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

package com.ververica.cdc.common.event;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * Class {@code DataChangeEvent} represents the data change events of external systems, such as
 * INSERT, UPDATE, DELETE and so on.
 */
@PublicEvolving
public interface DataChangeEvent extends ChangeEvent {

    /** Describes the record of data before change. */
    DataRecord before();

    /** Describes the record of data after change. */
    DataRecord after();

    /** Describes the operation type of the change event. e.g. INSERT, UPDATE, REPLACE, DELETE. */
    OperationType op();

    /** Optional, describes the metadata of the change event. e.g. MySQL binlog file name, pos. */
    Map<String, String> meta();
}
