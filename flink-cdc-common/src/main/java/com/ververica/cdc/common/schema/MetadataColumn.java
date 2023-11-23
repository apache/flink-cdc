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

package com.ververica.cdc.common.schema;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.types.DataType;

import javax.annotation.Nullable;

import java.util.Objects;

/** Representation of a metadata column. */
@PublicEvolving
public class MetadataColumn extends Column {

    private static final long serialVersionUID = 1L;

    private final @Nullable String metadataKey;

    public MetadataColumn(
            String name, DataType type, @Nullable String metadataKey, @Nullable String comment) {
        super(name, type, comment);
        this.metadataKey = metadataKey;
    }

    @Override
    public boolean isPhysical() {
        return false;
    }

    @Nullable
    public String getMetadataKey() {
        return metadataKey;
    }

    @Override
    public Column copy(DataType newType) {
        return new MetadataColumn(name, newType, metadataKey, comment);
    }

    @Override
    public Column copy(String newName) {
        return new MetadataColumn(newName, type, metadataKey, comment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetadataColumn)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MetadataColumn that = (MetadataColumn) o;
        return Objects.equals(metadataKey, that.metadataKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), metadataKey);
    }
}
