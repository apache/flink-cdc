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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.utils.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The projection of transform applies to describe a projection of filtering tables. Projection
 * includes the original columns of the data table and the user-defined computed columns.
 *
 * <p>A transformation projection contains:
 *
 * <ul>
 *   <li>projection: a string for projecting the row of matched table as output.
 *   <li>projectionColumns: a list for recording all columns transformation of the projection.
 * </ul>
 */
public class TransformProjection implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String projection;
    private final List<ProjectionColumn> projectionColumns;

    public TransformProjection(String projection, List<ProjectionColumn> projectionColumns) {
        this.projection = projection;
        this.projectionColumns = projectionColumns;
    }

    public String getProjection() {
        return projection;
    }

    public List<ProjectionColumn> getProjectionColumns() {
        return projectionColumns;
    }

    public boolean isValid() {
        return !StringUtils.isNullOrWhitespaceOnly(projection);
    }

    public static Optional<TransformProjection> of(String projection) {
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return Optional.empty();
        }
        return Optional.of(new TransformProjection(projection, new ArrayList<>()));
    }

    @Override
    public String toString() {
        return "TransformProjection{"
                + "projection='"
                + getProjection()
                + '\''
                + ", projectionColumns="
                + getProjectionColumns()
                + '}';
    }
}
