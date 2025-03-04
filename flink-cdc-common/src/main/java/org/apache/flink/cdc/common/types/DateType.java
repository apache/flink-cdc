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
import java.util.List;

/**
 * Data type of a date consisting of {@code year-month-day} with values ranging from {@code
 * 0000-01-01} to {@code 9999-12-31}. Compared to the SQL standard, the range starts at year {@code
 * 0000}.
 *
 * <p>A conversion from and to {@code int} describes the number of days since epoch.
 */
@PublicEvolving
public final class DateType extends DataType {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "DATE";

    public DateType(boolean isNullable) {
        super(isNullable, DataTypeRoot.DATE);
    }

    public DateType() {
        this(true);
    }

    @Override
    protected DataType copy(boolean isNullable) {
        return new DateType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
