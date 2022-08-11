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

import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Optional;

/**
 * Factory to create {@link DeserializationRuntimeConverter} according to {@link LogicalType}. It's
 * usually used to create a user-defined {@link DeserializationRuntimeConverter} which has a higher
 * resolve order than default converter.
 */
public interface DeserializationRuntimeConverterFactory extends Serializable {

    /** A user-defined converter factory which always fallback to default converters. */
    DeserializationRuntimeConverterFactory DEFAULT =
            (logicalType, serverTimeZone) -> Optional.empty();

    /**
     * Returns an optional {@link DeserializationRuntimeConverter}. Returns {@link Optional#empty()}
     * if fallback to default converter.
     *
     * @param logicalType the Flink Table & SQL internal datatype to be converted from objects of
     *     Debezium
     * @param serverTimeZone TimeZone used to convert data with timestamp type
     */
    Optional<DeserializationRuntimeConverter> createUserDefinedConverter(
            LogicalType logicalType, ZoneId serverTimeZone);
}
