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

package org.apache.flink.cdc.composer.definition;

import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
import org.apache.flink.cdc.common.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Definition of a transformation.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>sourceTable: a regex pattern for matching input table IDs. Required for the definition.
 *   <li>projection: a string for projecting the row of matched table as output. Optional for the
 *       definition.
 *   <li>filter: a string for filtering the row of matched table as output. Optional for the
 *       definition.
 *   <li>primaryKeys: a string for primary key columns for matching input table IDs, separated by
 *       `,`. Optional for the definition.
 *   <li>partitionKeys: a string for partition key columns for matching input table IDs, separated
 *       by `,`. Optional for the definition.
 *   <li>tableOptions: a string for table options for matching input table IDs, options are
 *       separated by `,`, key and value are separated by `=`. Optional for the definition.
 *   <li>tableOptionsDelimiter: a string for delimiter of table options, default is `,`. Optional
 *       for the definition.
 *   <li>description: description for the transformation. Optional for the definition.
 *   <li>column-name-case (YAML): optional per-transform override for pipeline {@code
 *       column-name-case} (AS_IS, UPPER, LOWER). Same key as pipeline shorthand. When set, it
 *       applies to this rule's post-transform schema only.
 * </ul>
 *
 * <p><b>Post-transform registration:</b> A rule is registered on the post-transform operator only
 * if {@link #registersPostTransformRule()} is true (non-empty projection/filter or per-transform
 * {@code column-name-case}). Fields such as {@code primary-keys}, {@code partition-keys}, or {@code
 * converter-after-transform} are interpreted in the post-transform phase and have no effect unless
 * a projection/filter or {@code column-name-case} is also set.
 */
public class TransformDef {
    private final String sourceTable;
    private final String projection;
    private final String filter;
    private final String description;
    private final String primaryKeys;
    private final String partitionKeys;
    private final String tableOptions;
    private final String tableOptionsDelimiter;
    private final String postTransformConverter;

    /** When non-null, overrides pipeline {@code column-name-case} for this transform. */
    private final @Nullable SchemaColumnCaseFormat columnCaseFormat;

    public TransformDef(
            String sourceTable,
            String projection,
            String filter,
            String primaryKeys,
            String partitionKeys,
            String tableOptions,
            String tableOptionsDelimiter,
            String description,
            String postTransformConverter) {
        this(
                sourceTable,
                projection,
                filter,
                primaryKeys,
                partitionKeys,
                tableOptions,
                tableOptionsDelimiter,
                description,
                postTransformConverter,
                null);
    }

    public TransformDef(
            String sourceTable,
            String projection,
            String filter,
            String primaryKeys,
            String partitionKeys,
            String tableOptions,
            String tableOptionsDelimiter,
            String description,
            String postTransformConverter,
            @Nullable SchemaColumnCaseFormat columnCaseFormat) {
        this.sourceTable = sourceTable;
        this.projection = projection;
        this.filter = filter;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.tableOptions = tableOptions;
        this.tableOptionsDelimiter = tableOptionsDelimiter;
        this.description = description;
        this.postTransformConverter = postTransformConverter;
        this.columnCaseFormat = columnCaseFormat;
    }

    public TransformDef(
            String sourceTable,
            String projection,
            String filter,
            String primaryKeys,
            String partitionKeys,
            String tableOptions,
            String description,
            String postTransformConverter) {
        this(
                sourceTable,
                projection,
                filter,
                primaryKeys,
                partitionKeys,
                tableOptions,
                ",",
                description,
                postTransformConverter,
                null);
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getProjection() {
        return projection;
    }

    public boolean isValidProjection() {
        return !StringUtils.isNullOrWhitespaceOnly(projection);
    }

    public String getFilter() {
        return filter;
    }

    public boolean isValidFilter() {
        return !StringUtils.isNullOrWhitespaceOnly(filter);
    }

    /**
     * Returns true if this transform has a non-empty projection or filter, so it is registered on
     * the post-transform operator. Otherwise post-only settings (e.g. {@code primary-keys} in YAML)
     * do not take effect for that phase.
     */
    public boolean registersPostTransformRule() {
        return isValidProjection() || isValidFilter() || columnCaseFormat != null;
    }

    public String getDescription() {
        return description;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public String getPartitionKeys() {
        return partitionKeys;
    }

    public String getTableOptions() {
        return tableOptions;
    }

    public String getTableOptionsDelimiter() {
        return tableOptionsDelimiter;
    }

    public String getPostTransformConverter() {
        return postTransformConverter;
    }

    @Nullable
    public SchemaColumnCaseFormat getColumnCaseFormat() {
        return columnCaseFormat;
    }

    @Override
    public String toString() {
        return "TransformDef{"
                + "sourceTable='"
                + sourceTable
                + '\''
                + ", projection='"
                + projection
                + '\''
                + ", filter='"
                + filter
                + '\''
                + ", description='"
                + description
                + '\''
                + ", postTransformConverter='"
                + postTransformConverter
                + '\''
                + ", columnCaseFormat="
                + columnCaseFormat
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformDef that = (TransformDef) o;
        return Objects.equals(sourceTable, that.sourceTable)
                && Objects.equals(projection, that.projection)
                && Objects.equals(filter, that.filter)
                && Objects.equals(description, that.description)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(tableOptions, that.tableOptions)
                && Objects.equals(tableOptionsDelimiter, that.tableOptionsDelimiter)
                && Objects.equals(postTransformConverter, that.postTransformConverter)
                && columnCaseFormat == that.columnCaseFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceTable,
                projection,
                filter,
                description,
                primaryKeys,
                partitionKeys,
                tableOptions,
                tableOptionsDelimiter,
                postTransformConverter,
                columnCaseFormat);
    }
}
