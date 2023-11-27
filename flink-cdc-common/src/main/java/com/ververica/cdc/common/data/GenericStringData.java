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

package com.ververica.cdc.common.data;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.utils.StringUtf8Utils;

import javax.annotation.Nonnull;

import java.util.Objects;

/** A internal data structure representing data of {@link VarCharType} and {@link CharType}. */
@PublicEvolving
public final class GenericStringData implements StringData {

    private final String javaStr;

    private GenericStringData(String javaStr) {
        this.javaStr = javaStr;
    }

    @Override
    public byte[] toBytes() {
        if (javaStr == null) {
            return null;
        } else {
            return StringUtf8Utils.encodeUTF8(javaStr);
        }
    }

    @Override
    public int compareTo(@Nonnull StringData o) {
        GenericStringData other = (GenericStringData) o;
        return javaStr.compareTo(other.javaStr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericStringData)) {
            return false;
        }
        GenericStringData that = (GenericStringData) o;
        return Objects.equals(javaStr, that.javaStr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaStr);
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    public static GenericStringData fromString(String javaStr) {
        return new GenericStringData(javaStr);
    }

    /** Copy a new {@code GenericStringData}. */
    public GenericStringData copy() {
        return fromString(javaStr);
    }

    @Override
    public String toString() {
        return javaStr;
    }
}
