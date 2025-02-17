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

import java.util.Objects;

/** Describes the raw data type in jdbc data sources. */
public class JdbcRawDataType implements RawDataType {

    private static final long serialVersionUID = 1L;

    private final int jdbcType;
    private final String typeName;
    private final int length;
    private final Integer scale;

    public JdbcRawDataType(int jdbcType, String typeName, int length, Integer scale) {
        this.jdbcType = jdbcType;
        this.typeName = typeName;
        this.length = length;
        this.scale = scale;
    }

    /** Returns the {@link java.sql.Types}. */
    public int getJdbcType() {
        return jdbcType;
    }

    /** Returns the type name. */
    public String getTypeName() {
        return typeName;
    }

    /** Returns the maximum length. For numeric data types, this represents the precision. */
    public int getLength() {
        return length;
    }

    /** Returns the scale. */
    public Integer getScale() {
        return scale;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JdbcRawDataType that = (JdbcRawDataType) obj;
        return jdbcType == that.jdbcType
                && Objects.equals(typeName, that.typeName)
                && length == that.length
                && Objects.equals(scale, that.scale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcType, typeName, length, scale);
    }
}
