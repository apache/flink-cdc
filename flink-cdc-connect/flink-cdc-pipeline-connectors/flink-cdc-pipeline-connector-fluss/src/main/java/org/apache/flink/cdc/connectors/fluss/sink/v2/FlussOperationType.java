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

package org.apache.flink.cdc.connectors.fluss.sink.v2;

/* This file is based on source code of Apache Fluss Project (https://fluss.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Enumeration of row operation types used in Fluss-Flink data processing.
 *
 * <p>This enum represents the type of operation associated with a row, such as an append (insert),
 * upsert (update or insert), delete, or ignore. It is used to indicate how a row should be
 * interpreted or processed in downstream systems.
 *
 * <ul>
 *   <li>{@link #APPEND} - Represents an append-only (insert) operation.
 *   <li>{@link #UPSERT} - Represents an upsert operation (update or insert).
 *   <li>{@link #DELETE} - Represents a delete operation.
 *   <li>{@link #IGNORE} - Represents an operation that should be ignored.
 * </ul>
 *
 * @see FlussRowWithOp
 */
public enum FlussOperationType {
    /** Represents an append-only (insert) operation. */
    APPEND,

    /** Represents an upsert operation (update or insert). */
    UPSERT,

    /** Represents a delete operation. */
    DELETE,

    /** Represents an operation that should be ignored. */
    IGNORE
}
