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

package org.apache.flink.cdc.common.types;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * An enumeration of data type roots containing static information about fata data types.
 *
 * <p>A root is an essential description of a {@link DataType} without additional parameters. For
 * example, a parameterized data type {@code DECIMAL(12,3)} possesses all characteristics of its
 * root {@code DECIMAL}. Additionally, a data type root enables efficient comparison during the
 * evaluation of types.
 *
 * <p>See the type-implementing classes for a more detailed description of each type.
 *
 * <p>Note to implementers: Whenever we perform a match against a type root (e.g. using a
 * switch/case statement), it is recommended to:
 *
 * <ul>
 *   <li>Order the items by the type root definition in this class for easy readability.
 *   <li>Think about the behavior of all type roots for the implementation. A default fallback is
 *       dangerous when introducing a new type root in the future.
 * </ul>
 */
@PublicEvolving
public enum DataTypeRoot {
    CHAR(DataTypeFamily.PREDEFINED, DataTypeFamily.CHARACTER_STRING),

    VARCHAR(DataTypeFamily.PREDEFINED, DataTypeFamily.CHARACTER_STRING),

    BOOLEAN(DataTypeFamily.PREDEFINED),

    BINARY(DataTypeFamily.PREDEFINED, DataTypeFamily.BINARY_STRING),

    VARBINARY(DataTypeFamily.PREDEFINED, DataTypeFamily.BINARY_STRING),

    DECIMAL(DataTypeFamily.PREDEFINED, DataTypeFamily.NUMERIC, DataTypeFamily.EXACT_NUMERIC),

    TINYINT(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    SMALLINT(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    INTEGER(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    BIGINT(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    FLOAT(DataTypeFamily.PREDEFINED, DataTypeFamily.NUMERIC, DataTypeFamily.APPROXIMATE_NUMERIC),

    DOUBLE(DataTypeFamily.PREDEFINED, DataTypeFamily.NUMERIC, DataTypeFamily.APPROXIMATE_NUMERIC),

    DATE(DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME),

    TIME_WITHOUT_TIME_ZONE(DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME, DataTypeFamily.TIME),

    TIMESTAMP_WITHOUT_TIME_ZONE(
            DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME, DataTypeFamily.TIMESTAMP),

    TIMESTAMP_WITH_TIME_ZONE(
            DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME, DataTypeFamily.TIMESTAMP),

    TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.DATETIME,
            DataTypeFamily.TIMESTAMP,
            DataTypeFamily.EXTENSION),

    ARRAY(DataTypeFamily.CONSTRUCTED, DataTypeFamily.COLLECTION),

    MAP(DataTypeFamily.CONSTRUCTED, DataTypeFamily.EXTENSION),

    ROW(DataTypeFamily.CONSTRUCTED);

    private final Set<DataTypeFamily> families;

    DataTypeRoot(DataTypeFamily firstFamily, DataTypeFamily... otherFamilies) {
        this.families = Collections.unmodifiableSet(EnumSet.of(firstFamily, otherFamilies));
    }

    public Set<DataTypeFamily> getFamilies() {
        return families;
    }
}
