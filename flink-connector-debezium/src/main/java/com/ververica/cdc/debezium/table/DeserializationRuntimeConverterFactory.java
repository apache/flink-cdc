/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Factory to create {@link DeserializationRuntimeConverter} according to {@link LogicalType}. It's
 * usually used to create a customized {@link DeserializationRuntimeConverter} to convert the
 * objects of Debezium with a specific logic binding to database.
 */
public interface DeserializationRuntimeConverterFactory extends Serializable {

    /**
     * Create {@link DeserializationRuntimeConverter}.
     *
     * @param logicalType the Flink Table & SQL internal datatype to be converted from objects of
     *     Debezium
     * @param createConvertFunc the function called to create {@link
     *     DeserializationRuntimeConverter}, useful when need to get the underlying converter of
     *     nested data in a nest datatype such as {@link
     *     org.apache.flink.table.types.logical.ArrayType}
     */
    DeserializationRuntimeConverter create(
            LogicalType logicalType,
            Function<LogicalType, DeserializationRuntimeConverter> createConvertFunc);
}
