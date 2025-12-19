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

import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Fluss Project (https://fluss.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A wrapper class that associates an {@link InternalRow} with an {@link FlussOperationType} for use
 * in Fluss-Flink data processing.
 *
 * <p>This class is used to represent a row of data along with its corresponding operation type,
 * such as APPEND, UPSERT, or DELETE, as defined by {@link FlussOperationType}.
 *
 * @see InternalRow
 * @see FlussOperationType
 */
public class FlussRowWithOp {
    /** The internal row data. */
    private final InternalRow row;

    /** The type of operation associated with this row (e.g., APPEND, UPSERT, DELETE). */
    private final FlussOperationType opType;

    /**
     * Constructs a {@code RowWithOp} with the specified internal row and operation type.
     *
     * @param row the internal row data (must not be null)
     * @param opType the operation type (must not be null)
     * @throws NullPointerException if {@code row} or {@code opType} is null
     */
    public FlussRowWithOp(InternalRow row, @Nullable FlussOperationType opType) {
        this.row = checkNotNull(row, "row cannot be null");
        this.opType = checkNotNull(opType, "opType cannot be null");
    }

    /**
     * Returns the internal row data.
     *
     * @return the internal row
     */
    public InternalRow getRow() {
        return row;
    }

    /**
     * Returns the operation type associated with this row.
     *
     * @return the operation type
     */
    public FlussOperationType getOperationType() {
        return opType;
    }

    /**
     * Indicates whether some other object is "equal to" this one. Two {@code RowWithOp} objects are
     * considered equal if their internal rows and operation types are equal.
     *
     * @param o the reference object with which to compare
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussRowWithOp rowWithOp = (FlussRowWithOp) o;
        return Objects.equals(row, rowWithOp.row) && opType == rowWithOp.opType;
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(row, opType);
    }
}
