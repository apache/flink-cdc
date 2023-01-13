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

package com.ververica.cdc.connectors.mysql.debezium.task.context.exception;

/**
 * A wrapper class for clearly show the possible reason of a schema-out-of-sync exception thrown
 * inside Debezium.
 */
public class SchemaOutOfSyncException extends Exception {
    public SchemaOutOfSyncException(String message, Throwable cause) {
        super(message, cause);
    }
}
