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

package com.ververica.cdc.connectors.oceanbase.source;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** The definition of the chunk bound. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OceanBaseSnapshotChunkBound implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final OceanBaseSnapshotChunkBound START_BOUND =
            new OceanBaseSnapshotChunkBound(ChunkBoundType.START, null);
    public static final OceanBaseSnapshotChunkBound END_BOUND =
            new OceanBaseSnapshotChunkBound(ChunkBoundType.END, null);

    @JsonProperty private ChunkBoundType boundType;
    @JsonProperty private List<Object> value;

    public OceanBaseSnapshotChunkBound() {}

    public OceanBaseSnapshotChunkBound(ChunkBoundType boundType, @Nullable List<Object> value) {
        this.boundType = boundType;
        this.value = value;
    }

    public ChunkBoundType getBoundType() {
        return boundType;
    }

    public void setBoundType(ChunkBoundType boundType) {
        this.boundType = boundType;
    }

    public List<Object> getValue() {
        return value;
    }

    public void setValue(List<Object> value) {
        this.value = value;
    }

    public static OceanBaseSnapshotChunkBound middleOf(List<Object> obj) {
        return new OceanBaseSnapshotChunkBound(ChunkBoundType.MIDDLE, obj);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OceanBaseSnapshotChunkBound)) {
            return false;
        }
        OceanBaseSnapshotChunkBound that = (OceanBaseSnapshotChunkBound) o;
        if (boundType != that.boundType) {
            return false;
        }
        if (value == null) {
            return that.value == null;
        }
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(boundType, value);
    }

    /** The type of the chunk bound. */
    public enum ChunkBoundType {
        START,
        MIDDLE,
        END
    }
}
