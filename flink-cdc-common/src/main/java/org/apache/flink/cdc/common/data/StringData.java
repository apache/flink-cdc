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

package org.apache.flink.cdc.common.data;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.VarCharType;

/** An internal data structure representing data of {@link CharType} and {@link VarCharType}. */
@PublicEvolving
public interface StringData extends Comparable<StringData> {

    /**
     * Converts this {@link StringData} object to a UTF-8 byte array.
     *
     * <p>Note: The returned byte array may be reused.
     */
    byte[] toBytes();

    /** Converts this {@link StringData} object to a {@link String}. */
    String toString();
}
