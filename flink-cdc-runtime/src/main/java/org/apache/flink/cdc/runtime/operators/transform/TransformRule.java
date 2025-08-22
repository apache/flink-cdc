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

import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A rule defining pre-transformations where filtered rows and irrelevant columns are removed. */
public class TransformRule implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tableInclusions;
    private final @Nullable String projection;
    private final @Nullable String filter;
    private final String primaryKey;
    private final String partitionKey;
    private final String tableOption;
    private final @Nullable String postTransformConverter;
    private final SupportedMetadataColumn[] supportedMetadataColumns;

    public TransformRule(
            String tableInclusions,
            @Nullable String projection,
            @Nullable String filter,
            String primaryKey,
            String partitionKey,
            String tableOption,
            @Nullable String postTransformConverter,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        this.tableInclusions = tableInclusions;
        this.projection = StringUtils.isNullOrWhitespaceOnly(projection) ? "*" : projection;
        this.filter = filter;
        this.primaryKey = primaryKey;
        this.partitionKey = partitionKey;
        this.tableOption = tableOption;
        this.postTransformConverter = postTransformConverter;
        this.supportedMetadataColumns = supportedMetadataColumns;
    }

    public String getTableInclusions() {
        return tableInclusions;
    }

    @Nullable
    public String getProjection() {
        return projection;
    }

    @Nullable
    public String getFilter() {
        return filter;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getTableOption() {
        return tableOption;
    }

    @Nullable
    public String getPostTransformConverter() {
        return postTransformConverter;
    }

    public SupportedMetadataColumn[] getSupportedMetadataColumns() {
        return supportedMetadataColumns;
    }
}
