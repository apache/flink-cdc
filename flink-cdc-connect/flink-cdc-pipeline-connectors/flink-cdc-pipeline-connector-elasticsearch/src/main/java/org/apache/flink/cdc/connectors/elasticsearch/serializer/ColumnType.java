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

package org.apache.flink.cdc.connectors.elasticsearch.serializer;

/**
 * Enumeration of column types supported in the Elasticsearch connector. These types represent the
 * various data types that can be used in database columns and are relevant for serialization and
 * deserialization processes.
 */
public enum ColumnType {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    CHAR,
    VARCHAR,
    BINARY,
    VARBINARY,
    DECIMAL,
    DATE,
    TIME_WITHOUT_TIME_ZONE,
    TIMESTAMP_WITHOUT_TIME_ZONE,
    TIMESTAMP_WITH_LOCAL_TIME_ZONE,
    TIMESTAMP_WITH_TIME_ZONE,
    ARRAY,
    MAP,
    ROW
}
