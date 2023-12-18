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

package com.ververica.cdc.common.types;

import com.ververica.cdc.common.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Data type of a fixed-length binary string (=a sequence of bytes).
 *
 * <p>For expressing a zero-length binary string literal, this type does also support {@code n} to
 * be 0. However, this is not exposed through the API.
 */
@PublicEvolving
public final class BinaryType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int EMPTY_LITERAL_LENGTH = 0;

    public static final int MIN_LENGTH = 1;

    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    public static final int DEFAULT_LENGTH = 1;

    private static final String FORMAT = "BINARY(%d)";

    private final int length;

    public BinaryType(boolean isNullable, int length) {
        super(isNullable, DataTypeRoot.BINARY);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Binary string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    public BinaryType(int length) {
        this(true, length);
    }

    public BinaryType() {
        this(DEFAULT_LENGTH);
    }

    public int getLength() {
        return length;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new BinaryType(isNullable, length);
    }

    @Override
    public String asSerializableString() {
        if (length == EMPTY_LITERAL_LENGTH) {
            throw new IllegalArgumentException(
                    "Zero-length binary strings have no serializable string representation.");
        }
        return withNullability(FORMAT, length);
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, length);
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BinaryType that = (BinaryType) o;
        return length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), length);
    }
}
