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

package org.apache.flink.cdc.connectors.pgvector.utils;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Type declaration for a pgvector column. */
public class PgVectorColumnSpec implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Pattern TYPE_PATTERN =
            Pattern.compile("(?i)\\s*(vector|halfvec|sparsevec|bit)\\s*\\((\\d+)\\)\\s*");

    private final PgVectorType type;
    private final int dimension;

    public PgVectorColumnSpec(PgVectorType type, int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("Vector dimension must be greater than 0.");
        }
        this.type = Objects.requireNonNull(type);
        this.dimension = dimension;
    }

    public static PgVectorColumnSpec parse(String value) {
        Matcher matcher = TYPE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Vector column type must use vector(n), halfvec(n), sparsevec(n), or bit(n): "
                            + value);
        }
        return new PgVectorColumnSpec(
                PgVectorType.valueOf(matcher.group(1).toUpperCase(Locale.ROOT)),
                Integer.parseInt(matcher.group(2)));
    }

    public PgVectorType getType() {
        return type;
    }

    public int getDimension() {
        return dimension;
    }

    public String toSqlType() {
        return type.name().toLowerCase(Locale.ROOT) + "(" + dimension + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PgVectorColumnSpec)) {
            return false;
        }
        PgVectorColumnSpec that = (PgVectorColumnSpec) o;
        return dimension == that.dimension && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, dimension);
    }

    @Override
    public String toString() {
        return toSqlType();
    }
}
