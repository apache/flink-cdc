/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.debezium.table;

import org.apache.flink.annotation.Internal;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/** A converter converts {@link SourceRecord} metadata into Flink internal data structures. */
@FunctionalInterface
@Internal
public interface MetadataConverter extends Serializable {
    Object read(SourceRecord record);
}
