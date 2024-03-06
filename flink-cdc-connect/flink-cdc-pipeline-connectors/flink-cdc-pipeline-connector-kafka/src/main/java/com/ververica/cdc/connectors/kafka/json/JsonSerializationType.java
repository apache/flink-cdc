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

package com.ververica.cdc.connectors.kafka.json;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.connectors.kafka.json.canal.CanalJsonSerializationSchema;
import com.ververica.cdc.connectors.kafka.json.debezium.DebeziumJsonSerializationSchema;

/** type of {@link SerializationSchema} to serialize {@link Event} for kafka. */
public enum JsonSerializationType {

    /** Use {@link DebeziumJsonSerializationSchema} to serialize. */
    DEBEZIUM_JSON("debezium-json"),

    /** Use {@link CanalJsonSerializationSchema} to serialize. */
    CANAL_JSON("canal-json");

    private final String value;

    JsonSerializationType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
